"""Gmail polling service for Indeed/Jimoty job application notifications."""
import imaplib
import email
from email.header import decode_header
from email.utils import parseaddr
import os
import socket
import time
import json
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set, Tuple
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

# gmail_oauth は「python src/main.py」（Procfile・src/ がトップに乗る）でも
# 「from src.main import ...」（テスト・リポジトリルートが乗る）でも import できるよう両対応。
try:
    from src import gmail_oauth  # tests / パッケージとして import された場合
except ImportError:  # pragma: no cover - 実行形態による分岐
    import gmail_oauth  # `python src/main.py` で src/ が sys.path 先頭にある場合

# --- Env Vars (Railway Variables) ---
GMAIL_IMAP_HOST = os.getenv("GMAIL_IMAP_HOST", "imap.gmail.com")
GMAIL_IMAP_USER = os.getenv("GMAIL_IMAP_USER")
GMAIL_IMAP_PASSWORD = os.getenv("GMAIL_IMAP_PASSWORD")

# --- OAuth2 (refresh token) 認証 ---
# アプリパスワード(IMAP)は数週間で失効再発するため、原則失効しない OAuth2 refresh token
# 方式へ移行する（[[feedback_google_auth_standard]]）。OAuth の資格情報が揃っていれば
# IMAP XOAUTH2 で認証し、未設定ならアプリパスワード IMAP にフォールバックする（移行期の安全網）。
# 実体は src/gmail_oauth.py。利用する Gmail アドレスは GMAIL_IMAP_USER を共用する。
# OAuth 有効化条件は CLIENT_ID + REFRESH_TOKEN（client_secret も実質必須・本番実機確認済み）。
# 必要スコープ = https://mail.google.com/（gmail.readonly は IMAP XOAUTH2 で AUTHENTICATIONFAILED）。
# 監視メールボックス（GMAIL_IMAP_USER）= atsuhiro@takagi.bz（recruit@takagi.bz は存在しない）。
def use_oauth() -> bool:
    """OAuth2(refresh token) 方式が利用可能か（資格情報が全て設定済みか）。"""
    return gmail_oauth.has_oauth_credentials()

# MODE-based configuration
MODE = os.getenv("MODE", "prod")  # "test" or "prod" (default: prod)
SLACK_WEBHOOK_URL_TEST = os.getenv("SLACK_WEBHOOK_URL_TEST")
SLACK_WEBHOOK_URL_PROD = os.getenv("SLACK_WEBHOOK_URL_PROD")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_TO_ID_TEST = os.getenv("LINE_TO_ID_TEST")
LINE_TO_ID_PROD = os.getenv("LINE_TO_ID_PROD")
LOG_DIR = os.getenv("LOG_DIR", "/tmp")
SLACK_ERROR_WEBHOOK_URL = os.getenv("SLACK_ERROR_WEBHOOK_URL")
SLACK_DM_WEBHOOK_URL = os.getenv("SLACK_DM_WEBHOOK_URL")

# --- Processed IDs file for duplicate prevention ---
PROCESSED_IDS_FILE = os.getenv("PROCESSED_IDS_FILE", os.path.join(LOG_DIR, "processed_ids.json"))
MAX_PROCESSED_IDS = 5000


# --- Polling Interval ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))  # デフォルト60秒（Gmail API制限対策）
MAX_BACKOFF_SECONDS = int(os.getenv("MAX_BACKOFF_SECONDS", "900"))  # 最大15分のバックオフ

# --- Search window for emails (days) ---
SEARCH_DAYS = int(os.getenv("SEARCH_DAYS", "1"))  # デフォルト1日間（Gmail API制限対策）

# --- Batch limit per cycle (QUOTA ERROR対策) ---
MAX_EMAILS_PER_CYCLE = int(os.getenv("MAX_EMAILS_PER_CYCLE", "10"))  # 1サイクルで処理する最大メール数

# --- IMAP robustness ---
IMAP_TIMEOUT_SECONDS = int(os.getenv("IMAP_TIMEOUT_SECONDS", "30"))
IMAP_RETRY_BACKOFFS = [5, 10, 20]  # 接続失敗時のexponential backoff（秒）。長さがリトライ回数。

# --- Error notification deduplication ---
ERROR_NOTIFICATION_DEDUP_SECONDS = int(os.getenv("ERROR_NOTIFICATION_DEDUP_SECONDS", "600"))  # 同一エラーの再通知抑制（10分）
# 認証失敗の通知抑制（既定6時間）。アプリパスワード失効は人手による再発行が必要で
# すぐには直らないため、10分おきに通知を垂れ流さず長めの間隔に集約する。
AUTH_ERROR_NOTIFICATION_DEDUP_SECONDS = int(os.getenv("AUTH_ERROR_NOTIFICATION_DEDUP_SECONDS", "21600"))
_last_error_notification_ts: dict = {}  # dedup_key -> last unix timestamp


def is_auth_failure(error: object) -> bool:
    """IMAP のエラーが「認証失敗（資格情報が無効/失効）」かどうかを判定する。

    Gmail はアプリパスワードが失効/削除されると
    `[AUTHENTICATIONFAILED] Invalid credentials (Failure)` を返す。
    これはネットワーク瞬断などの一時障害とは異なり、人手によるアプリパスワード
    再発行が必要なため、通常の接続エラーとは別扱いで分かりやすく通知する。
    """
    text = str(error).upper()
    return (
        "AUTHENTICATIONFAILED" in text
        or "INVALID CREDENTIALS" in text
        # OAuth2: refresh token が失効/取消されると invalid_grant が返る（要再同意）。
        or "INVALID_GRANT" in text
    )


def has_imap_credentials() -> bool:
    """IMAP 接続に必要な資格情報が設定されているか。

    OAuth2(refresh token) 方式が揃っていれば GMAIL_IMAP_USER のみで足りる
    （パスワードは access token で代替）。OAuth が未設定の場合は従来どおり
    ユーザー名＋アプリパスワードの両方が必要。
    """
    if not GMAIL_IMAP_USER:
        return False
    if use_oauth():
        return True
    return bool(GMAIL_IMAP_PASSWORD)

# --- Known Indeed non-application email patterns (silently ignored) ---
# These are legitimate Indeed emails that are NOT job applications.
# Add new patterns here as they are discovered.
# Indeed由来だが「応募通知ではない」既知の件名パターン（=静かにスキップする除外リスト）。
# 方針: 除外側は厳密に（応募通知の件名と衝突しない固有フレーズだけを入れる）。
# 「新しい応募者のお知らせ」等の本物の応募通知の件名には絶対に含まれない語のみを列挙すること。
INDEED_NON_APPLICATION_PATTERNS = [
    # --- 求人レコメンド・掲載/パフォーマンス・課金系 ---
    "オススメ求人が",
    "求人への応募状況をお知らせします",
    "応募状況レポート",
    "求人パフォーマンス",
    "求人の掲載が",
    "Indeed請求",
    "お支払い",
    "求人についての最新情報",
    "求人広告の",
    "Indeedからのお知らせ",
    " @ ",  # 求人おすすめメール（「求人名 @ 会社名」形式）
    # --- アカウント・ログイン・セキュリティ系（2段階認証コード等） ---
    # 例:「認証コード (xxxxxx) を入力してIndeedにログインしてください」
    "認証コード",
    "確認コード",
    "ログインコード",
    "セキュリティコード",
    "ワンタイムパスワード",
    "ログインしてください",
    "ログインリクエスト",
    "サインイン",
    "パスワードの再設定",
    "パスワードをリセット",
    "二段階認証",
    "2段階認証",
    "アカウントの保護",
    "新しいデバイス",
    # --- 配信設定・通知系 ---
    "配信を停止",
    "メール配信設定",
]


# --- Logging ---
def log(msg: str) -> None:
    """Log message to file and stdout."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    with open(os.path.join(LOG_DIR, "recruit.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def ensure_processed_ids_dir() -> bool:
    """Ensure the directory for processed IDs file exists. Returns True if successful."""
    try:
        parent_dir = Path(PROCESSED_IDS_FILE).parent
        if not parent_dir.exists():
            parent_dir.mkdir(parents=True, exist_ok=True)
            log(f"Created directory: {parent_dir}")
        return True
    except OSError as e:
        log(f"ERROR: Failed to create directory for processed IDs: {e}")
        notify_error_to_slack(f"Failed to create directory for processed IDs: {e}")
        return False


def migrate_old_id_format(ids: Set[str]) -> Set[str]:
    """Migrate old ID format (raw numbers) to new format (gm:xxx prefix).

    Old format: "12345678901234567890"
    New format: "gm:12345678901234567890" or "mid:<message-id@example.com>"
    """
    migrated = set()
    migration_count = 0
    for id_value in ids:
        if id_value.startswith("gm:") or id_value.startswith("mid:"):
            # Already in new format
            migrated.add(id_value)
        elif id_value.isdigit():
            # Old format (raw X-GM-MSGID number) - migrate to new format
            migrated.add(f"gm:{id_value}")
            migration_count += 1
        else:
            # Unknown format, keep as-is (could be old Message-ID without prefix)
            migrated.add(id_value)
    if migration_count > 0:
        log(f"Migrated {migration_count} IDs from old format to new format")
    return migrated


def load_processed_ids() -> Tuple[Set[str], bool]:
    """Load processed message IDs from file.

    Returns:
        Tuple of (processed_ids set, success flag).
        If file exists but can't be read, returns (empty set, False)
        to prevent mass re-processing.
    """
    if os.path.exists(PROCESSED_IDS_FILE):
        try:
            with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            log(f"Loaded {len(data)} processed IDs from {PROCESSED_IDS_FILE}")
            # Migrate old format IDs to new format
            original_set = set(data)
            migrated = migrate_old_id_format(original_set)
            # Save immediately if migration occurred to prevent re-migration on crash
            if migrated != original_set:
                save_processed_ids(migrated)
            return migrated, True
        except (json.JSONDecodeError, IOError) as e:
            log(f"ERROR: Failed to load processed IDs (file exists but corrupted): {e}")
            notify_error_to_slack(f"CRITICAL: Failed to load processed IDs - file corrupted: {e}")
            # Return False to prevent mass re-processing of all emails
            return set(), False
    else:
        log(f"Processed IDs file does not exist: {PROCESSED_IDS_FILE} (first run)")
        return set(), True


def save_processed_ids(processed_ids: Set[str]) -> bool:
    """Save processed message IDs to file atomically. Returns True if successful.

    Uses tempfile + os.replace() for atomic write to prevent JSON corruption on crash.
    Note: uid: entries are session-only cache and are NOT persisted to disk.
    Only gm: and mid: entries (which provide deduplication correctness) are saved.
    This prevents unbounded file growth from uid: cache accumulation.
    """
    if not ensure_processed_ids_dir():
        return False
    try:
        # Exclude uid: entries - they are session-only cache, gm:/mid: entries handle dedup
        persistent_ids = [item for item in processed_ids if not item.startswith("uid:")]
        # Trim to MAX_PROCESSED_IDS to prevent unbounded growth
        if len(persistent_ids) > MAX_PROCESSED_IDS:
            persistent_ids.sort()
            persistent_ids = persistent_ids[-MAX_PROCESSED_IDS:]
        # Atomic write: write to temp file then replace to prevent partial writes on crash
        target_path = Path(PROCESSED_IDS_FILE)
        tmp_path = target_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(persistent_ids, f)
        tmp_path.replace(target_path)
        log(f"Saved {len(persistent_ids)} processed IDs to {PROCESSED_IDS_FILE} (excluded {len(processed_ids) - len(persistent_ids)} uid: cache entries)")
        return True
    except IOError as e:
        log(f"ERROR: Failed to save processed IDs: {e}")
        notify_error_to_slack(f"Failed to save processed IDs: {e}")
        return False


def notify_error_to_slack(message: str, dedup_key: Optional[str] = None,
                          dedup_seconds: Optional[int] = None) -> None:
    """重大なエラーを Slack Webhook に通知する。

    Args:
        message: 通知メッセージ
        dedup_key: 重複抑止用キー。指定時、同一キーで前回通知から
            dedup_seconds（既定 ERROR_NOTIFICATION_DEDUP_SECONDS=600秒=10分）以内なら
            スキップする。省略時は message 本文を dedup key として使う。
        dedup_seconds: 重複抑止の窓（秒）。省略時は ERROR_NOTIFICATION_DEDUP_SECONDS。
            認証失効など長く続く障害は長めの窓を指定して通知フラッドを防ぐ。
    """
    key = dedup_key if dedup_key is not None else message
    window = dedup_seconds if dedup_seconds is not None else ERROR_NOTIFICATION_DEDUP_SECONDS
    now_ts = time.time()
    last_ts = _last_error_notification_ts.get(key, 0.0)
    if now_ts - last_ts < window:
        log(f"Skipping duplicate error notification within {window}s window: key={key[:80]}")
        return
    _last_error_notification_ts[key] = now_ts

    webhook_url = SLACK_ERROR_WEBHOOK_URL or SLACK_WEBHOOK_URL_PROD
    if not webhook_url:
        log("ERROR: No Slack webhook URL configured; cannot notify error to Slack")
        return
    text = f"🚨 Indeed応募通知エラー発生\n{message}"
    try:
        resp = requests.post(
            webhook_url,
            json={"text": text},
            timeout=5,
        )
        if resp.status_code >= 400:
            log(f"ERROR: failed to send error notification to Slack (status={resp.status_code}, body={resp.text})")
    except Exception as e:
        # 通知時のエラーでさらに例外を投げるとループするのでログのみ
        log(f"ERROR: exception while sending error notification to Slack: {e}")

    # DM にも同じメッセージを送信
    if SLACK_DM_WEBHOOK_URL:
        try:
            dm_resp = requests.post(
                SLACK_DM_WEBHOOK_URL,
                json={"text": text},
                timeout=5,
            )
            if dm_resp.status_code >= 400:
                log(f"ERROR: failed to send error DM to Slack (status={dm_resp.status_code})")
        except Exception as e:
            log(f"ERROR: exception while sending error DM to Slack: {e}")


# --- MODE management ---
def is_test_mode() -> bool:
    """Check if running in test mode."""
    return (MODE.lower() if MODE else "prod") == "test"


def get_slack_webhook_url() -> Optional[str]:
    """Get Slack Webhook URL based on current mode."""
    url = SLACK_WEBHOOK_URL_TEST if is_test_mode() else SLACK_WEBHOOK_URL_PROD
    if not url:
        log(f"WARNING: SLACK_WEBHOOK_URL_{'TEST' if is_test_mode() else 'PROD'} is not set")
    return url


def get_line_to_id() -> Optional[str]:
    """Get LINE TO ID based on current mode."""
    to_id = LINE_TO_ID_TEST if is_test_mode() else LINE_TO_ID_PROD
    if not to_id:
        log(f"WARNING: LINE_TO_ID_{'TEST' if is_test_mode() else 'PROD'} is not set")
    return to_id


def add_test_prefix(message: str) -> str:
    """Add test version prefix if in test mode."""
    return f"【テストバージョン】\n{message}" if is_test_mode() else message


# --- Email Parsing ---
def decode_header_value(value: Optional[str]) -> str:
    """Decode email header value."""
    if not value:
        return ""
    parts = decode_header(value)
    return "".join(
        text.decode(enc or "utf-8", errors="replace") if isinstance(text, bytes) else text
        for text, enc in parts
    )


def extract_name(from_header: Optional[str]) -> str:
    """Extract applicant name from From header."""
    if not from_header:
        return "Unknown"
    try:
        return from_header.split("<")[0].replace('"', "").strip()
    except (IndexError, AttributeError):
        return from_header


def extract_html(msg: email.message.Message) -> str:
    """Extract HTML content from email message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                charset = part.get_content_charset() or "utf-8"
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(charset, errors="replace")
    elif msg.get_content_type() == "text/html":
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(charset, errors="replace")
    return ""


def extract_indeed_url(html: str) -> str:
    """Extract application URL from Indeed email HTML."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a"):
        if "応募内容を確認する" in (a.get_text() or ""):
            return a.get("href") or ""
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if "indeed" in href:
            return href
    return ""


def extract_applicant_name_from_html(html: str) -> Optional[str]:
    """IndeedメールのHTML本文から応募者名を抽出する。

    Indeedのメールはfrom_headerが「Indeed <noreply@indeed.com>」のため
    ヘッダーからは応募者名を取得できない。代わりにメール本文HTMLから取得する。

    試みるパターン:
    1. 「○○さんからの応募」「○○ さんが応募しました」等のテキスト
    2. 件名「新しい応募者のお知らせ: ○○」のパターン
    3. td/div/p内に「応募者:」「応募者名:」等のラベルに続く名前
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")

    # パターン1: 「○○さんからの応募」「○○さんが応募しました」
    for pattern in [
        r"([^\s　\n]+(?:\s[^\s　\n]+)?)\s*さん(?:から(?:の)?応募|が応募)",
        r"新しい応募者(?:のお知らせ)?[:：]\s*([^\n\r]+)",
        r"応募者(?:名)?[:：]\s*([^\n\r]+)",
        r"([^\s　\n]{1,20})\s*(?:様|さん)(?:\s|$|が|から|の)",
    ]:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # 明らかに名前ではないものを除外（URLや長すぎる文字列）
            if name and len(name) <= 30 and "http" not in name and "@" not in name:
                return name

    return None


def extract_job_title_from_subject(subject: str) -> Optional[str]:
    """Indeed件名から求人名を抽出する。

    Indeed件名パターン例:
    - 「新しい応募者のお知らせ: 警備員 | 日本交通誘導」
    - 「応募がありました - 警備員」
    「:」や「-」の後の求人名部分を返す。「|」以降の会社名は除外。
    """
    for pattern in [
        r"(?:のお知らせ|がありました)\s*[：:\-‐]\s*([^|\n\r]+?)(?:\s*\||\s*$)",
        r"新しい応募者[：:]\s*([^|\n\r]+?)(?:\s*\||\s*$)",
        r"応募者[のお知らせ]+[：:]\s*([^|\n\r]+?)(?:\s*\||\s*$)",
    ]:
        match = re.search(pattern, subject)
        if match:
            title = match.group(1).strip()
            if title and len(title) <= 60:
                return title
    return None


def extract_job_title_from_html(html: str) -> Optional[str]:
    """IndeedメールHTML本文から求人名を抽出する。"""
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")

    for pattern in [
        r"求人(?:名|タイトル)?[：:]\s*([^\n\r]+)",
        r"Job[Tt]itle[：:]\s*([^\n\r]+)",
        r"ポジション[：:]\s*([^\n\r]+)",
        r"に応募がありました",  # このパターンの直前行が求人名の可能性あり
    ]:
        match = re.search(pattern, text)
        if match and match.lastindex:
            title = match.group(1).strip()
            if title and len(title) <= 60 and "http" not in title:
                return title
    return None


def _shorten_via_tinyurl(encoded_url: str) -> Optional[str]:
    """Try to shorten a URL via TinyURL. Returns shortened URL or None."""
    try:
        api = "https://tinyurl.com/api-create.php?url=" + encoded_url
        resp = requests.get(api, timeout=8)
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
        log(f"WARN: TinyURL shorten returned status={resp.status_code}")
    except Exception as e:
        log(f"WARN: TinyURL shorten failed: {e}")
    return None


def _shorten_via_isgd(encoded_url: str) -> Optional[str]:
    """Try to shorten a URL via is.gd. Returns shortened URL or None."""
    try:
        api = "https://is.gd/create.php?format=simple&url=" + encoded_url
        resp = requests.get(api, timeout=5)
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
        log(f"WARN: is.gd shorten returned status={resp.status_code} body={resp.text[:80]}")
    except Exception as e:
        log(f"WARN: is.gd shorten failed: {e}")
    return None


def shorten_url(url: str) -> str:
    """Shorten URL using TinyURL and is.gd with domain-aware ordering.

    is.gd blocks Indeed domains entirely, so for Indeed URLs we use
    TinyURL as primary. For other URLs, is.gd is preferred (faster).
    Returns the original URL if all shorteners fail.
    """
    if not url:
        return url
    encoded = requests.utils.quote(url, safe="")
    is_indeed = "indeed" in url.lower()
    if is_indeed:
        # is.gd blocks all *.indeed.com domains → TinyURL first
        result = _shorten_via_tinyurl(encoded) or _shorten_via_isgd(encoded)
    else:
        result = _shorten_via_isgd(encoded) or _shorten_via_tinyurl(encoded)
    if result:
        return result
    log("WARN: Both URL shorteners failed, using original URL")
    return url


# --- Notification Functions ---
def notify_slack_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, max_retries: int = 3) -> bool:
    """Send notification to Slack with retry logic. Returns True if successful."""
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        log("No Slack Webhook URL")
        return False
    title = "【Indeed応募】" if source == "indeed" else "【ジモティー】"
    mention_prefix = "<!channel>\n"

    lines = [f"{title} 【{name}】 さんから応募がありました。"]
    if job_title:
        lines.append(f"求人: {job_title}")
    if url:
        lines.extend(["", "応募内容はこちら:", shorten_url(url)])
    message = add_test_prefix(mention_prefix + "\n".join(lines))
    for attempt in range(max_retries):
        try:
            resp = requests.post(webhook_url, json={"text": message}, timeout=10)
            if resp.status_code < 400:
                return True
            log(f"ERROR: Slack notify failed (status={resp.status_code}, body={resp.text}, attempt={attempt + 1}/{max_retries})")
        except requests.exceptions.Timeout:
            log(f"ERROR: Slack notify timeout (attempt={attempt + 1}/{max_retries})")
        except Exception as e:
            log(f"ERROR: Slack notify exception: {e} (attempt={attempt + 1}/{max_retries})")
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
    notify_error_to_slack(f"Slack notify failed after {max_retries} attempts for {name}")
    return False


def notify_line_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, max_retries: int = 3) -> bool:
    """Send notification to LINE with retry logic. Returns True if successful."""
    line_to_id = get_line_to_id()
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return False
    title = "Indeedに応募がありました。" if source == "indeed" else "ジモティーで新着があります。"
    lines = [f"【{name}】 さんから{title}"]
    if job_title:
        lines.append(f"求人: {job_title}")
    if url:
        # Force LINE to open URL in external browser (Chrome/Safari)
        # to avoid Google OAuth blocking in LINE's in-app browser
        separator = "&" if "?" in url else "?"
        external_url = f"{url}{separator}openExternalBrowser=1"
        lines.extend(["", "詳細はこちら:", shorten_url(external_url)])
    base_message = add_test_prefix("\n".join(lines))
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    def _build_body_v2() -> dict:
        substitution = {
            "mention_all": {
                "type": "mention",
                "mentionee": {"type": "all"},
            }
        }
        return {
            "to": line_to_id,
            "messages": [{"type": "textV2", "text": "{mention_all} " + base_message, "substitution": substitution}],
        }

    def _build_body_plain() -> dict:
        # textV2 フォールバック時: {mention_all} リテラルを除去して plain text を送信
        plain_text = base_message.replace("{mention_all} ", "").replace("{mention_all}", "")
        return {
            "to": line_to_id,
            "messages": [{"type": "text", "text": plain_text}],
        }

    for attempt in range(max_retries):
        # textV2 (@all mention) を試み、失敗したら plain text にフォールバック
        for body_builder, label in [(_build_body_v2, "textV2+mention"), (_build_body_plain, "text(fallback)")]:
            try:
                resp = requests.post("https://api.line.me/v2/bot/message/push", json=body_builder(), headers=headers, timeout=10)
                log(f"LINE API response: status={resp.status_code} type={label}")
                if resp.status_code < 400:
                    return True
                log(f"LINE notify {label} failed (status={resp.status_code}, body={resp.text[:200]})")
                if resp.status_code == 400:
                    # 400はtextV2未対応や不正メンション → 内側ループの次builder(plain)へ即フォールバック
                    continue
                # 4xx以外（5xx等）はリトライ
                break
            except requests.exceptions.Timeout:
                log(f"ERROR: LINE notify timeout type={label} (attempt={attempt + 1}/{max_retries})")
                break
            except Exception as e:
                log(f"ERROR: LINE notify exception type={label}: {e}")
                break
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)
    notify_error_to_slack(f"LINE notify failed after {max_retries} attempts for {name}")
    return False


# --- IMAP Connection ---
# Backoff schedule for IMAP (re)connection attempts (seconds).
IMAP_CONNECT_BACKOFF_SECONDS = [5, 10, 20]


class IMAPConnectionPool:
    """Single-connection pool that reuses an IMAP connection across polls.

    A fresh IMAP login per poll cycle is wasteful and brittle: Gmail
    throttles frequent logins, and the TLS + LOGIN round-trip burns time on
    every cycle. This pool keeps one connection alive and verifies liveness
    via NOOP before handing it out. On any failure, the stale connection is
    closed and a new one is established with exponential backoff
    (5s, 10s, 20s — three attempts in total).
    """

    def __init__(self) -> None:
        self._connection: Optional[imaplib.IMAP4_SSL] = None

    def get(self) -> imaplib.IMAP4_SSL:
        """Return a live IMAP connection, reusing the existing one if alive."""
        if self._connection is not None:
            if self._is_alive(self._connection):
                return self._connection
            log(f"IMAP pooled connection to {GMAIL_IMAP_HOST} is dead; reconnecting")
            self._close_silently()
        self._connection = self._connect_with_backoff()
        return self._connection

    def reset(self) -> None:
        """Force-discard the current connection (call after an IMAP error)."""
        self._close_silently()

    def _is_alive(self, mail: imaplib.IMAP4_SSL) -> bool:
        try:
            status, _ = mail.noop()
            return status == "OK"
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            return False

    def _close_silently(self) -> None:
        if self._connection is None:
            return
        try:
            self._connection.close()
        except Exception:
            pass
        try:
            self._connection.logout()
        except Exception:
            pass
        self._connection = None

    def _connect_with_backoff(self) -> imaplib.IMAP4_SSL:
        delays = IMAP_CONNECT_BACKOFF_SECONDS
        max_attempts = len(delays)
        last_exc: Optional[BaseException] = None
        for attempt in range(1, max_attempts + 1):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            log(f"IMAP connect attempt {attempt}/{max_attempts} host={GMAIL_IMAP_HOST} ts={ts}")
            try:
                mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
                if use_oauth():
                    # OAuth2(refresh token) → IMAP XOAUTH2。原則失効しないので
                    # アプリパスワードの再発行が不要になる。
                    gmail_oauth.imap_authenticate(mail, GMAIL_IMAP_USER)
                    auth_method = "XOAUTH2"
                else:
                    # フォールバック: 従来のアプリパスワード IMAP（移行期の安全網）。
                    mail.login(GMAIL_IMAP_USER, GMAIL_IMAP_PASSWORD)
                    auth_method = "app-password"
                mail.select("INBOX", readonly=True)
                log(f"IMAP connect success host={GMAIL_IMAP_HOST} auth={auth_method} attempt={attempt}/{max_attempts}")
                return mail
            # RuntimeError/RequestException は OAuth トークン取得失敗（refresh token 失効や
            # ネットワーク断）。接続失敗と同様に backoff＋上位の認証失敗判定に乗せる。
            except (imaplib.IMAP4.error, OSError, RuntimeError, requests.RequestException) as e:
                last_exc = e
                fail_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                log(
                    f"ERROR: IMAP connect failed host={GMAIL_IMAP_HOST} "
                    f"attempt={attempt}/{max_attempts} ts={fail_ts} error={e!r}"
                )
                if attempt < max_attempts:
                    backoff = delays[attempt - 1]
                    log(f"Backing off {backoff}s before IMAP reconnect attempt {attempt + 1}")
                    time.sleep(backoff)
        log(
            f"ERROR: IMAP connect exhausted {max_attempts} attempts host={GMAIL_IMAP_HOST} "
            f"last_error={last_exc!r}"
        )
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("IMAP connect failed with no exception captured")


_imap_pool = IMAPConnectionPool()


@contextmanager
def imap_connection():
    """Yield a pooled IMAP connection, reconnecting with backoff on failure.

    The yielded connection is owned by the module-level pool and is NOT
    closed on successful exit — the next caller reuses it. Any IMAP/socket
    error inside the with-block invalidates the pooled connection so the
    next call reconnects with exponential backoff.
    """
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)
    try:
        mail = _imap_pool.get()
        try:
            yield mail
        except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
            _imap_pool.reset()
            raise
    finally:
        socket.setdefaulttimeout(old_timeout)


# --- Mail Processing ---
def parse_fetch_response(data: list) -> Tuple[Optional[str], Optional[bytes]]:
    """Parse IMAP fetch response to extract X-GM-MSGID and body."""
    gm_msgid = None
    body_data = None
    for item in data:
        if isinstance(item, tuple):
            header = item[0].decode() if isinstance(item[0], bytes) else item[0]
            if "X-GM-MSGID" in header:
                match = re.search(r"X-GM-MSGID (\d+)", header)
                if match:
                    gm_msgid = match.group(1)
            # Only set body_data on first non-None value to avoid overwriting with later empty tuples
            if body_data is None and len(item) > 1:
                body_data = item[1]
    return gm_msgid, body_data


# Indeedの「応募通知」と判定する件名パターン（=応募側は広めに）。
# Indeedが件名フォーマットを多少変えても応募を取りこぼさないよう、本物の応募通知に
# 固有な語を複数登録する。除外リスト(INDEED_NON_APPLICATION_PATTERNS)と衝突しない語に限る。
INDEED_APPLICATION_PATTERNS = [
    "新しい応募者のお知らせ",
    "新しい応募者",       # 「新しい応募者が1名います」等の言い回し変化に追従
    "応募がありました",
    "応募者からのメッセージ",
    "応募を受け付けました",   # 件名フォーマット変化への追従
    "新規応募",
    "応募が届きました",
]

# Indeed が件名フォーマットを変えても応募通知を取りこぼさないための正規表現フォールバック。
# 固定フレーズ（INDEED_APPLICATION_PATTERNS）に一致しなくても、「応募」と
# 「あった/来た/受付/届いた/者」等の動き・対象を表す語が同じ件名に共起していれば応募とみなす。
# 誤検知防止のため、この共起判定は INDEED_NON_APPLICATION_PATTERNS（レコメンド・課金・
# 認証コード等）に一致しない件名にのみ適用する。
_INDEED_APPLICATION_FALLBACK_RE = re.compile(
    r"応募(?:者|を|が|の)?.{0,12}"
    r"(?:ありました|きました|来ました|届きました|受け付け|受付|お知らせ|通知|1名|名います)"
)


def _looks_like_indeed_application(subject: str) -> bool:
    """件名が Indeed の応募通知らしいかを、固定フレーズ＋正規表現フォールバックで判定する。

    既知の非応募パターン（認証コード・課金・レコメンド等）に該当する件名は、
    フォーマット変化に強くしても応募と誤判定しないよう先に除外する。
    """
    if any(pattern in subject for pattern in INDEED_APPLICATION_PATTERNS):
        return True
    if is_indeed_non_application_email(subject):
        return False
    return bool(_INDEED_APPLICATION_FALLBACK_RE.search(subject))


def determine_source(subject: str) -> Tuple[Optional[str], Optional[str]]:
    """Determine email source and default URL based on subject."""
    if _looks_like_indeed_application(subject):
        return "indeed", None
    elif "ジモティー" in subject:
        return "jimoty", "https://jmty.jp/web_mail/posts"
    return None, None


# Indeed が実際にメールを送ってくるドメイン（送信者の「実アドレス」で判定する）。
# 表示名に "Indeed" が入っているだけ（例: 'Indeed' via 株式会社○○ <info@employer.com>）や、
# 件名に "indeed" の文字列を含むだけ（例: GitHub通知メールでブランチ名に indeed が入る）の
# メールを Indeed 扱いして誤アラートを出さないために、ホワイトリスト方式で厳格に判定する。
INDEED_SENDER_DOMAINS = (
    "indeed.com",
    "indeedemail.com",
)


def is_from_indeed(from_header: str) -> bool:
    """送信者が本当に Indeed のドメインかを、表示名ではなく From の実アドレスで判定する。

    例:
      "Indeed <noreply@indeed.com>"                         -> True
      "'Indeed' via 株式会社日本交通誘導 <info@kotsuyudo.com>" -> False（実アドレスは employer ドメイン）
      "GitHub <notifications@github.com>"（件名に indeed）    -> False
    """
    addr = parseaddr(from_header or "")[1].lower()
    if "@" not in addr:
        return False
    domain = addr.rsplit("@", 1)[-1]
    return any(domain == d or domain.endswith("." + d) for d in INDEED_SENDER_DOMAINS)


def is_indeed_non_application_email(subject: str) -> bool:
    """Check if an Indeed email is a known non-application type.

    These include recommendation emails, status reports, billing notices, etc.
    Returns True if the subject matches any known non-application pattern.
    """
    return any(pattern in subject for pattern in INDEED_NON_APPLICATION_PATTERNS)


def get_unique_id(gm_msgid: Optional[str], msg: email.message.Message) -> Optional[str]:
    """Get unique identifier for email. Prefers X-GM-MSGID, falls back to Message-ID."""
    if gm_msgid:
        return f"gm:{gm_msgid}"
    message_id = msg.get("Message-ID")
    if message_id:
        return f"mid:{message_id}"
    return None


def process_mail_by_uid(
    mail: imaplib.IMAP4_SSL,
    uid: bytes,
    processed_ids: Set[str]
) -> Optional[str]:
    """Process a single mail by UID. Returns unique ID if processed, None otherwise."""
    uid_str = uid.decode() if isinstance(uid, bytes) else uid

    # Use UID FETCH instead of regular FETCH
    status, data = mail.uid("fetch", uid_str, "(X-GM-MSGID BODY.PEEK[])")
    if status != "OK":
        log(f"ERROR: Failed to fetch uid={uid_str}, status={status}")
        return None

    gm_msgid, body_data = parse_fetch_response(data)
    if not body_data:
        log(f"ERROR: Failed to fetch body for uid={uid_str}")
        return None

    msg = email.message_from_bytes(body_data)

    # Get unique identifier (X-GM-MSGID or Message-ID)
    unique_id = get_unique_id(gm_msgid, msg)
    if not unique_id:
        log(f"ERROR: No unique ID found for uid={uid_str}, skipping to prevent duplicates")
        return None

    # Double-check with X-GM-MSGID/Message-ID (in case UID tracking missed it)
    if unique_id in processed_ids:
        log(f"Already processed (skip): uid={uid_str}, id={unique_id}")
        return None

    subject = decode_header_value(msg.get("Subject", ""))
    from_header = decode_header_value(msg.get("From", ""))

    source, default_url = determine_source(subject)
    if not source:
        # 「Indeed扱い」は送信者の実ドメインだけで判定する（件名/表示名の "indeed" 文字列では判定しない）。
        # これにより、件名に "indeed" を含むだけのGitHub通知メールや、表示名だけ "Indeed" の
        # 認証コード/セキュリティ通知（実アドレスは別ドメイン）に対して誤アラートを出さない。
        if is_from_indeed(from_header):
            # 送信者は本当にIndeedだが応募通知ではない
            if is_indeed_non_application_email(subject):
                # 既知の非応募パターン（求人レコメンド、応募状況レポート等）→ 静かにスキップ
                log(f"Skip Indeed non-application mail: {subject[:80]}")
                return unique_id  # 処理済みマーク、アラートなし
            else:
                # 未知のパターン → Indeedが件名フォーマットを変更した可能性があるためアラート送信
                date_header = decode_header_value(msg.get("Date", ""))
                alert_msg = (
                    "⚠️ 件名不一致のIndeedメールを検知\n"
                    f"件名: {subject}\n"
                    f"From: {from_header}\n"
                    f"日時: {date_header}\n"
                    "Indeedが件名フォーマットを変更した可能性があります。determine_source関数の更新を検討してください。"
                )
                log(f"ALERT: Indeed email detected with unrecognized subject: {subject}")
                notify_error_to_slack(alert_msg)
                return unique_id  # アラート1回送信後は処理済みマーク（繰り返し通知を防止）
        else:
            log(f"Skip non-target mail: {subject[:50]}...")
            return unique_id  # Indeed以外の対象外メールは静かにスキップ＋処理済みマーク

    html = extract_html(msg)
    url = extract_indeed_url(html) if source == "indeed" else default_url

    # IndeedメールはFrom=「Indeed <noreply@indeed.com>」なので
    # メール本文HTMLから応募者名を取得する。取れなければFromヘッダーの名前を使う。
    if source == "indeed":
        applicant_name = extract_applicant_name_from_html(html)
        if not applicant_name:
            applicant_name = extract_name(from_header)
        # 求人名: 件名から優先的に取得、なければHTML本文から取得
        job_title = extract_job_title_from_subject(subject) or extract_job_title_from_html(html)
    else:
        applicant_name = extract_name(from_header)
        job_title = None

    log(f"Notify {source}: {applicant_name}, job={job_title}, url={url}, id={unique_id}")

    slack_ok = notify_slack_with_retry(source, applicant_name, url, job_title=job_title)
    line_ok = notify_line_with_retry(source, applicant_name, url, job_title=job_title)

    if not slack_ok and not line_ok:
        log(f"ERROR: All notifications failed for {applicant_name} ({unique_id}), will retry next cycle")
        return None

    # 片方成功・片方失敗: 処理済みマークしつつDMで通知（重複送信防止が最優先）
    if not slack_ok or not line_ok:
        failed_channels = []
        if not slack_ok:
            failed_channels.append("Slack")
        if not line_ok:
            failed_channels.append("LINE")
        log(f"WARNING: Partial success for {applicant_name} ({unique_id}): {', '.join(failed_channels)} failed. Marking as processed to prevent duplicates.")
        dm_detail = (
            f"\u26a0\ufe0f 通知一部失敗\uff08処理済みマーク済み・手動確認してください\uff09\n"
            f"失敗チャンネル: {', '.join(failed_channels)}\n"
            f"メール件名: {subject}\n"
            f"送信者: {from_header}\n"
            f"応募者名: {applicant_name}\n"
            f"ソース: {source}\n"
            f"unique_id: {unique_id}"
        )
        if SLACK_DM_WEBHOOK_URL:
            try:
                requests.post(SLACK_DM_WEBHOOK_URL, json={"text": dm_detail}, timeout=5)
            except Exception as e:
                log(f"ERROR: Failed to send detail DM: {e}")

    return unique_id


def get_gm_msgid_lightweight(mail: imaplib.IMAP4_SSL, uid: str) -> Optional[str]:
    """Fetch only X-GM-MSGID for a single email (lightweight, no body)."""
    status, data = mail.uid("fetch", uid, "(X-GM-MSGID)")
    if status != "OK":
        return None
    for item in data:
        if isinstance(item, tuple):
            header = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
        elif isinstance(item, bytes):
            header = item.decode(errors="replace")
        else:
            continue
        match = re.search(r"X-GM-MSGID (\d+)", header)
        if match:
            return f"gm:{match.group(1)}"
    return None


def _check_mail_attempt(processed_ids: Set[str]) -> None:
    """1サイクル分のメール処理本体。接続/IMAPエラーは呼び出し側でリトライさせるため再送出する。

    成功・部分成功・スキップは全て return（None）で抜ける。
    """
    with imap_connection() as mail:
        since_date = (datetime.now(timezone.utc) - timedelta(days=SEARCH_DAYS)).strftime("%d-%b-%Y")

        # Use UID SEARCH for stable identifiers
        status, data = mail.uid("search", None, "SINCE", since_date)
        if status != "OK":
            log(f"ERROR: UID SEARCH failed with status: {status}")
            return
        uid_list = data[0].split()
        log(f"Emails in last {SEARCH_DAYS} days: {len(uid_list)}")

        # Phase 1: Quick filter by UID (for emails we've seen before)
        uids_to_check = []
        for uid in uid_list:
            uid_str = uid.decode() if isinstance(uid, bytes) else uid
            if f"uid:{uid_str}" not in processed_ids:
                uids_to_check.append(uid)

        if not uids_to_check:
            return  # All emails already processed

        log(f"UIDs not in cache: {len(uids_to_check)}")

        # Phase 2: Lightweight check - fetch only X-GM-MSGID to filter by gm: prefix
        # This avoids full FETCH for emails that are already processed but missing uid: entry
        truly_new_uids = []
        uids_to_mark = []  # UIDs that are already processed but need uid: entry added

        for uid in uids_to_check:
            uid_str = uid.decode() if isinstance(uid, bytes) else uid
            gm_id = get_gm_msgid_lightweight(mail, uid_str)
            if gm_id and gm_id in processed_ids:
                # Already processed (has gm: entry), just need to add uid: entry
                uids_to_mark.append((uid_str, gm_id))
            else:
                # Truly new email, needs full processing
                truly_new_uids.append(uid)

        # Add uid: entries for already-processed emails (bootstrap)
        if uids_to_mark:
            log(f"Bootstrapping {len(uids_to_mark)} UIDs for already-processed emails")
            for uid_str, gm_id in uids_to_mark:
                processed_ids.add(f"uid:{uid_str}")
            if not save_processed_ids(processed_ids):
                log("ERROR: Failed to save bootstrapped UIDs")
                return

        if truly_new_uids:
            total_new = len(truly_new_uids)
            # QUOTA ERROR対策: 1サイクルで処理するメール数を制限する
            batch = truly_new_uids[:MAX_EMAILS_PER_CYCLE]
            if total_new > MAX_EMAILS_PER_CYCLE:
                log(f"Truly new emails to process: {total_new} (processing {MAX_EMAILS_PER_CYCLE} this cycle, {total_new - MAX_EMAILS_PER_CYCLE} deferred)")
            else:
                log(f"Truly new emails to process: {total_new}")

            # Phase 3: Full processing for truly new emails only (batch limited)
            for uid in batch:
                uid_str = uid.decode() if isinstance(uid, bytes) else uid
                unique_id = process_mail_by_uid(mail, uid, processed_ids)
                if unique_id:
                    # Store both the unique_id (gm: or mid:) and the uid for efficient filtering
                    processed_ids.add(unique_id)
                    processed_ids.add(f"uid:{uid_str}")
                    # Save immediately after each email to prevent duplicates on crash
                    if not save_processed_ids(processed_ids):
                        # If save fails, stop processing to prevent more potential duplicates
                        log("ERROR: Stopping mail processing due to save failure")
                        return


def check_mail_with_status() -> bool:
    """Check mailbox for new applications. Returns True if successful, False if quota/error.

    IMAP 接続/読み取りタイムアウトに対しては IMAP_RETRY_BACKOFFS（5,10,20秒）で
    リトライし、全て失敗した時のみ notify_error_to_slack() で通知する。
    """
    try:
        # 資格情報が未設定なら、リトライで叩かず即座に分かりやすく通知する
        if not has_imap_credentials():
            missing = []
            if not GMAIL_IMAP_USER:
                missing.append("GMAIL_IMAP_USER")
            # OAuth も アプリパスワードも無い → どちらの認証手段も成立しない。
            if not GMAIL_IMAP_PASSWORD:
                missing.append("GMAIL_IMAP_PASSWORD（または GMAIL_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN）")
            log(f"ERROR: Missing IMAP credentials: {', '.join(missing)}")
            notify_error_to_slack(
                "Gmail の資格情報が未設定です。\n"
                f"未設定の環境変数: {', '.join(missing)}\n"
                "推奨: OAuth2 方式（GMAIL_OAUTH_CLIENT_ID / GMAIL_OAUTH_REFRESH_TOKEN / "
                "GMAIL_OAUTH_CLIENT_SECRET）を Railway の Variables に設定してください。"
                "スコープは https://mail.google.com/ が必須（gmail.readonly は IMAP で失敗）。"
                "監視アドレス（GMAIL_IMAP_USER）は atsuhiro@takagi.bz を使用。"
                "（アプリパスワード GMAIL_IMAP_PASSWORD は失効しやすいため非推奨）",
                dedup_key="imap_missing_credentials",
                dedup_seconds=AUTH_ERROR_NOTIFICATION_DEDUP_SECONDS,
            )
            return True  # 設定待ち。quotaエラーではないので過度なbackoffはしない

        processed_ids, load_success = load_processed_ids()

        # If file exists but is corrupted, skip processing to prevent mass re-notifications
        if not load_success:
            log("ERROR: Skipping mail check due to corrupted processed IDs file")
            return True  # Not a quota error, don't backoff

        total_attempts = len(IMAP_RETRY_BACKOFFS) + 1  # initial + len(backoffs) retries
        last_conn_error: Optional[BaseException] = None
        for attempt_idx in range(total_attempts):
            try:
                _check_mail_attempt(processed_ids)
                return True
            except imaplib.IMAP4.abort:
                # quota / abort は別ハンドラへ
                raise
            except (imaplib.IMAP4.error, socket.timeout, socket.gaierror, TimeoutError, ConnectionError, OSError, RuntimeError, requests.RequestException) as e:
                last_conn_error = e
                log(f"WARN: IMAP connection/read error on attempt {attempt_idx + 1}/{total_attempts}: {e}")
                if attempt_idx < len(IMAP_RETRY_BACKOFFS):
                    backoff = IMAP_RETRY_BACKOFFS[attempt_idx]
                    log(f"Retrying IMAP in {backoff}s (next attempt {attempt_idx + 2}/{total_attempts})...")
                    time.sleep(backoff)
                    continue
                # All retries exhausted
                log(f"ERROR: IMAP connection failed after {total_attempts} attempts: {last_conn_error}")
                if is_auth_failure(last_conn_error):
                    # 資格情報の失効/無効。OAuth 方式と アプリパスワード方式で対処が異なるため、
                    # 現在の認証方式に応じた案内を出す。接続瞬断とは別の分かりやすいメッセージ＋
                    # 長めの抑止窓で通知する。
                    if use_oauth():
                        auth_alert = (
                            "Gmail OAuth の認証に失敗しました（refresh token が無効/失効＝要再同意）。\n"
                            f"エラー: {last_conn_error}\n"
                            "対処: scripts/get-gmail-refresh-token.py で再度同意し、Railway 環境変数 "
                            "`GMAIL_OAUTH_REFRESH_TOKEN` を更新してください。"
                            "（refresh token は通常失効しませんが、取消・パスワード変更・"
                            "6か月未使用で無効になることがあります）"
                        )
                    else:
                        auth_alert = (
                            "Gmail IMAP の認証に失敗しました（資格情報が無効/失効）。\n"
                            f"エラー: {last_conn_error}\n"
                            "対処: OAuth2 方式（GMAIL_OAUTH_*）への移行を推奨します。"
                            "暫定対応として Google アカウントでアプリパスワードを再発行し、"
                            "Railway 環境変数 `GMAIL_IMAP_PASSWORD` を更新してください。"
                            "（アプリパスワードが Google 側で削除/失効した可能性があります）"
                        )
                    notify_error_to_slack(
                        auth_alert,
                        dedup_key="imap_auth_failure",
                        dedup_seconds=AUTH_ERROR_NOTIFICATION_DEDUP_SECONDS,
                    )
                else:
                    notify_error_to_slack(
                        f"Gmail IMAP connection error (after {total_attempts} attempts): {last_conn_error}",
                        dedup_key="imap_connection_error",
                    )
                return True  # Not necessarily a quota error
        return True

    except imaplib.IMAP4.abort as e:
        error_msg = str(e)
        if "OVERQUOTA" in error_msg:
            log(f"QUOTA ERROR: {error_msg}")
            return False  # Quota error, trigger backoff
        log(f"ERROR: IMAP abort: {e}")
        return False  # Other IMAP error, also backoff
    except Exception as e:
        log(f"ERROR: {e}")
        # Check if it's a quota-related error
        if "OVERQUOTA" in str(e):
            return False  # Quota error, trigger backoff
        notify_error_to_slack(f"Gmail polling error: {e}", dedup_key="gmail_polling_error")
        return True  # Non-quota error, don't backoff excessively


def verify_storage() -> bool:
    """Verify that storage is working correctly at startup."""
    log(f"=== Storage Verification ===")
    log(f"PROCESSED_IDS_FILE={PROCESSED_IDS_FILE}")
    parent_dir = Path(PROCESSED_IDS_FILE).parent
    log(f"Parent directory: {parent_dir}")
    log(f"Parent directory exists: {parent_dir.exists()}")

    if parent_dir.exists():
        try:
            # Try to list directory contents
            contents = list(parent_dir.iterdir())
            log(f"Directory contents: {[str(f) for f in contents]}")
        except OSError as e:
            log(f"ERROR: Cannot list directory: {e}")

    # Ensure directory exists
    if not ensure_processed_ids_dir():
        log("ERROR: Failed to ensure storage directory exists")
        return False

    # Test write
    test_file = parent_dir / ".write_test"
    try:
        test_file.write_text("test")
        test_file.unlink()
        log("Storage write test: PASSED")
    except OSError as e:
        log(f"ERROR: Storage write test FAILED: {e}")
        notify_error_to_slack(f"Storage write test failed: {e}")
        return False

    # Load existing processed IDs
    processed_ids, load_success = load_processed_ids()
    if not load_success:
        log("ERROR: Processed IDs file is corrupted")
        return False

    log(f"Currently tracking {len(processed_ids)} processed emails")
    log(f"=== Storage Verification Complete ===")
    return True


# --- Main loop ---
def main() -> None:
    """Main polling loop with exponential backoff for quota errors."""
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")
    log(f"MODE={MODE}, SEARCH_DAYS={SEARCH_DAYS}, MAX_BACKOFF_SECONDS={MAX_BACKOFF_SECONDS}, MAX_EMAILS_PER_CYCLE={MAX_EMAILS_PER_CYCLE}")
    log(f"Gmail auth method: {'OAuth2 (XOAUTH2 refresh token)' if use_oauth() else 'app-password (IMAP LOGIN)'}")

    # Verify storage is working
    if not verify_storage():
        log("CRITICAL: Storage verification failed. Exiting to prevent duplicate notifications.")
        notify_error_to_slack("CRITICAL: Storage verification failed at startup. Service stopped.")
        return

    consecutive_errors = 0
    quota_notified = False
    while True:
        try:
            success = check_mail_with_status()
            if success:
                consecutive_errors = 0
                quota_notified = False
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                # Error occurred, apply exponential backoff
                consecutive_errors += 1
                backoff = min(POLL_INTERVAL_SECONDS * (2 ** consecutive_errors), MAX_BACKOFF_SECONDS)
                log(f"Backoff: waiting {backoff} seconds (consecutive_errors={consecutive_errors})")
                # Notify once when quota error starts
                if not quota_notified:
                    notify_error_to_slack(f"Gmail quota exceeded. Applying backoff ({backoff}s). Will retry automatically.")
                    quota_notified = True
                time.sleep(backoff)
        except Exception as e:
            log(f"ERROR in main loop: {e}")
            consecutive_errors += 1
            backoff = min(POLL_INTERVAL_SECONDS * (2 ** consecutive_errors), MAX_BACKOFF_SECONDS)
            time.sleep(backoff)


if __name__ == "__main__":
    main()


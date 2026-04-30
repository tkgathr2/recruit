"""Gmail polling service for Indeed/Jimoty job application notifications."""
import imaplib
import email
from email.header import decode_header
import os
import socket
import time
import json
import re
import hmac
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set, Tuple
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
from flask import Flask, request as flask_request, jsonify
from threading import Thread, RLock
from datetime import datetime, timedelta

# --- Startup Protection (GLOBAL STATE) ---
_startup_time = datetime.now()
_first_cycle_done = False
_first_cycle_lock = RLock()  # Protect _first_cycle_done from race conditions

# --- CTK Expiry Notification (GLOBAL STATE) ---
_ctk_expired_notified = False
_ctk_expired_notified_lock = RLock()  # Prevent duplicate notifications

# --- Env Vars (Railway Variables) ---
GMAIL_IMAP_HOST = os.getenv("GMAIL_IMAP_HOST", "imap.gmail.com")
GMAIL_IMAP_USER = os.getenv("GMAIL_IMAP_USER")
GMAIL_IMAP_PASSWORD = os.getenv("GMAIL_IMAP_PASSWORD")

# MODE-based configuration
MODE = os.getenv("MODE", "prod")  # "test" or "prod" (default: prod)
SLACK_WEBHOOK_URL_TEST = os.getenv("SLACK_WEBHOOK_URL_TEST")
SLACK_WEBHOOK_URL_PROD = os.getenv("SLACK_WEBHOOK_URL_PROD")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_TO_ID_TEST = os.getenv("LINE_TO_ID_TEST")
LINE_TO_ID_PROD = os.getenv("LINE_TO_ID_PROD")
LINE_TO_ID_PERSONAL = os.getenv("LINE_TO_ID_PERSONAL")  # CTK通知用（個人LINE）
COWORK_WEBHOOK_TOKEN = os.getenv("COWORK_WEBHOOK_TOKEN", "")

# CTK更新フォームのベースURL（Railway のサービスURL）
RAILWAY_SERVICE_URL = os.getenv("RAILWAY_SERVICE_URL", "https://recruit-production-f2dc.up.railway.app")

_processed_ids_lock = RLock()  # Thread-safe access to processed_ids

LOG_DIR = os.getenv("LOG_DIR", "/tmp")
SLACK_ERROR_WEBHOOK_URL = os.getenv("SLACK_ERROR_WEBHOOK_URL")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
COWORK_QUEUE_CHANNEL = os.getenv("COWORK_QUEUE_CHANNEL", "C0B1D2757FS")

# --- Processed IDs file for duplicate prevention ---
PROCESSED_IDS_FILE = os.getenv("PROCESSED_IDS_FILE", os.path.join(LOG_DIR, "processed_ids.json"))

# --- CAS State Store (v2: Indeed応募信号管理) ---
CAS_STATE_FILE = os.path.join(LOG_DIR, "cas_state.json")
_cas_store = {}  # {signal_id: {"status": ..., "detected_at": ..., ...}}
_cas_store_lock = RLock()

# --- Mention IDs (generic slots: set as many as needed) ---
SLACK_MENTION_ID_1 = os.getenv("SLACK_MENTION_ID_1")
SLACK_MENTION_ID_2 = os.getenv("SLACK_MENTION_ID_2")
LINE_MENTION_ID_1 = os.getenv("LINE_MENTION_ID_1")
LINE_MENTION_ID_2 = os.getenv("LINE_MENTION_ID_2")

# --- Polling Interval ---
def _safe_int(env_var: str, default: int) -> int:
    """環境変数を安全にintに変換する。不正値の場合はデフォルト値を使用。"""
    raw = os.getenv(env_var)
    if raw is None:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        # Cannot use log() here (not yet defined), use print
        print(f"WARNING: {env_var}='{raw}' is not a valid integer, using default={default}", flush=True)
        return default


POLL_INTERVAL_SECONDS = _safe_int("POLL_INTERVAL_SECONDS", 20)  # デフォルト20秒
MAX_BACKOFF_SECONDS = _safe_int("MAX_BACKOFF_SECONDS", 900)  # 最大15分のバックオフ

# --- Search window for emails (days) ---
SEARCH_DAYS = _safe_int("SEARCH_DAYS", 1)  # デフォルト1日間（Gmail API制限対策）

# --- Batch limit per cycle (QUOTA ERROR対策) ---
MAX_EMAILS_PER_CYCLE = _safe_int("MAX_EMAILS_PER_CYCLE", 10)  # 1サイクルで処理する最大メール数

# --- Startup Protection Threshold ---
# 初回サイクルでこの数を超えるメールが見つかった場合、再起動後の重複通知を防ぐため静かにマーク
STARTUP_NEW_EMAIL_THRESHOLD = _safe_int("STARTUP_NEW_EMAIL_THRESHOLD", 3)
FALLBACK_TIMEOUT_SECONDS = _safe_int("FALLBACK_TIMEOUT_SECONDS", 300)
FALLBACK_CHECK_INTERVAL = _safe_int("FALLBACK_CHECK_INTERVAL", 30)

# --- Logging ---
def log(msg: str) -> None:
    """Log message to file and stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    Returns: Tuple of (processed_ids set, success flag).
    If file exists but can't be read, returns (empty set, False) to prevent mass re-processing.
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
    All entry types (uid:, gm:, mid:) are persisted to ensure deduplication correctness.
    Cap at MAX_PROCESSED_IDS, keeping NEWEST entries (sorted by numeric ID).
    """
    if not ensure_processed_ids_dir():
        return False
    try:
        MAX_PROCESSED_IDS = 10000
        if len(processed_ids) > MAX_PROCESSED_IDS:
            # Sort by recency: gm:/uid: have numeric IDs (larger = newer); mid: has no order
            def _sort_key(msg_id: str) -> int:
                if msg_id.startswith("gm:"):
                    try:
                        return int(msg_id[3:])
                    except ValueError:
                        return 0
                elif msg_id.startswith("uid:"):
                    try:
                        return int(msg_id[4:])
                    except ValueError:
                        return 0
                return 0  # mid: and unknown — treated as oldest, discarded first when trimming
            # Sort ascending → take last MAX_PROCESSED_IDS (highest/newest numeric IDs)
            persistent_ids = sorted(processed_ids, key=_sort_key)[-MAX_PROCESSED_IDS:]
            log(f"Trimmed processed IDs from {len(processed_ids)} to {MAX_PROCESSED_IDS} (kept newest)")
        else:
            persistent_ids = list(processed_ids)
        # Atomic write: write to temp file then replace to prevent partial writes on crash
        target_path = Path(PROCESSED_IDS_FILE)
        tmp_path = target_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(persistent_ids, f)
            tmp_path.replace(target_path)
        except Exception:
            # Clean up .tmp file on failure to avoid stale file accumulation
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return True
    except IOError as e:
        log(f"ERROR: Failed to save processed IDs: {e}")
        notify_error_to_slack(f"Failed to save processed IDs: {e}")
        return False


def save_processed_ids_with_merge(new_ids: Set[str]) -> bool:
    """Thread-safe save: reload file, merge new_ids, save atomically.
    Use this from check_mail_with_status() to avoid race with Flask /manage-processed.
    Lock hold time is ~milliseconds (file I/O only), not seconds (IMAP).
    """
    with _processed_ids_lock:
        current_ids, load_success = load_processed_ids()
        if not load_success:
            log("ERROR: Could not reload processed IDs for merge-save")
            return False
        current_ids.update(new_ids)
        return save_processed_ids(current_ids)

def notify_ctk_expired() -> None:
    """Indeed CTK 期限切れを LINE と Slack で通知する（1サービス起動中に1度だけ）。
    フラグは少なくとも1つの通知成功後に設定する。全失敗時はフラグをリセットして次回リトライ可能に。
    """
    global _ctk_expired_notified
    with _ctk_expired_notified_lock:
        if _ctk_expired_notified:
            return  # すでに通知済み
    # ← フラグはここでは設定しない（送信成功後に設定）
    log("ALERT: Indeed CTK が期限切れです。LINE/Slack に通知します。")
    setup_url = f"{RAILWAY_SERVICE_URL}/update-ctk-setup?token={COWORK_WEBHOOK_TOKEN}"
    session_setup_url = f"{RAILWAY_SERVICE_URL}/update-session-setup?token={COWORK_WEBHOOK_TOKEN}"
    message = (
        "⚠️ Indeed CTK が期限切れです\n\n"
        "電話番号・住所の取得ができません。\n"
        "※ 応募通知自体は届き続けます。\n\n"
        "【CTK更新手順】\n"
        "① Chrome で jp.indeed.com を開く\n"
        "② お気に入り →「CTK更新」をタップ\n"
        "③ CTK値が自動入力されたページが開く\n"
        "④「更新する」ボタンを押して完了\n\n"
        "【セッションCookie更新（電話番号取得）】\n"
        "① employers.indeed.com/candidates を開く\n"
        "② お気に入り →「Indeed セッション更新」をタップ\n\n"
        "※ CTK更新ブックマークがまだの場合は👇\n"
        f"{setup_url}\n\n"
        "※ セッション更新ブックマークがまだの場合は👇\n"
        f"{session_setup_url}"
    )
    notification_succeeded = False
    # Slack通知（notify_error_to_slack は🚨エラープレフィックスが付くので直接送信）
    if notify_slack_direct(message):
        log("CTK期限切れ Slack通知: 送信成功")
        notification_succeeded = True
    else:
        log("ERROR: CTK期限切れ Slack通知 失敗")
    # LINE通知（個人LINEに送信、未設定の場合はグループにフォールバック）
    line_to_id = LINE_TO_ID_PERSONAL or get_line_to_id()
    if LINE_CHANNEL_ACCESS_TOKEN and line_to_id:
        try:
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                json={"to": line_to_id, "messages": [{"type": "text", "text": message}]},
                headers={
                    "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            log(f"CTK期限切れ LINE通知: status={resp.status_code}")
            if resp.status_code < 400:
                notification_succeeded = True
        except Exception as e:
            log(f"ERROR: CTK期限切れ LINE通知 失敗: {e}")
    # 少なくとも1つ成功した場合のみフラグを設定（失敗時はフラグをリセットして次回リトライ）
    with _ctk_expired_notified_lock:
        if notification_succeeded:
            _ctk_expired_notified = True
        else:
            log("WARNING: CTK期限切れ通知が全て失敗。次回ポーリング時に再試行します。")


def notify_slack_direct(message: str) -> bool:
    """Slack Webhook にメッセージを送信する（エラープレフィックスなし）。Returns True if successful."""
    webhook_url = SLACK_ERROR_WEBHOOK_URL or SLACK_WEBHOOK_URL_PROD
    if not webhook_url:
        log("ERROR: No Slack webhook URL configured; cannot send Slack message")
        return False
    try:
        resp = requests.post(webhook_url, json={"text": message}, timeout=5)
        if resp.status_code >= 400:
            log(f"ERROR: Slack direct send failed (status={resp.status_code})")
            return False
        return True
    except Exception as e:
        log(f"ERROR: exception while sending Slack message: {e}")
        return False


def notify_error_to_slack(message: str) -> None:
    """重大なエラーを Slack Webhook に通知する（🚨エラープレフィックス付き）"""
    text = f"🚨 Indeed応募通知エラー発生\n{message}"
    notify_slack_direct(text)


def notify_url_missing(applicant_name: str, unique_id: str) -> None:
    """短縮URLが取得できなかった場合に LINE と Slack でアラートを送信する。
    URL未取得は重大エラー → 手動確認を促す。
    """
    log(f"ALERT: URL missing for {applicant_name} ({unique_id}) — sending alert")
    message = (
        f"⚠️ 【URL未取得アラート】\n\n"
        f"応募者: {applicant_name}\n"
        f"ID: {unique_id}\n\n"
        f"Indeed管理画面のURLが取得できませんでした。\n"
        f"手動でIndeed管理画面を確認してください。\n"
        f"https://employers.indeed.com/candidates"
    )
    # Slackアラート
    notify_slack_direct(message)
    # LINEアラート（個人LINEに送信）
    line_to_id = LINE_TO_ID_PERSONAL or get_line_to_id()
    if LINE_CHANNEL_ACCESS_TOKEN and line_to_id:
        try:
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                json={"to": line_to_id, "messages": [{"type": "text", "text": message}]},
                headers={
                    "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            log(f"URL missing LINE alert: status={resp.status_code}")
        except Exception as e:
            log(f"ERROR: URL missing LINE alert failed: {e}")

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
    """Decode email header value (RFC 2047).
    For encoded words: use declared charset, then fall back through common Japanese charsets.
    For unencoded bytes (enc=None): use us-ascii per RFC 2047, then fall back to utf-8.
    """
    if not value:
        return ""
    parts = decode_header(value)
    result = []
    for text, enc in parts:
        if isinstance(text, bytes):
            # Try declared charset first, then common Japanese fallbacks
            charsets = [enc] if enc else ["us-ascii"]
            charsets += ["utf-8", "iso-2022-jp", "shift_jis"]
            decoded = False
            for charset in charsets:
                try:
                    result.append(text.decode(charset))
                    decoded = True
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            if not decoded:
                result.append(text.decode("utf-8", errors="replace"))
        else:
            result.append(text)
    return "".join(result)

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

def extract_indeed_legacy_id(html: str) -> Optional[str]:
    """Indeed通知メールのHTMLからlegacyId（hex）を抽出する。
    Indeed通知メールには以下のURLパターンが含まれる:
    - https://employers.indeed.com/candidates/view?id=<legacyId>
    - https://engage.indeed.com/f/a/<legacyId>~~/... (旧形式: hex)
    - https://engage.indeed.com/f/a/<base64url>~~... (新形式: base64url 22文字)
    legacyId は hex文字列（8〜20桁）。
    """
    if not html:
        return None
    # パターン1: employers.indeed.com に直接 id= パラメータが含まれる場合
    direct = re.search(r'employers\.indeed\.com/candidates(?:/view)?\?(?:[^"\'<>\s]*&)?id=([a-f0-9]{8,20})', html)
    if direct:
        return direct.group(1)
    # パターン2: engage.indeed.com/f/a/<hex>~~ 形式（旧形式）
    engage_hex = re.search(r'engage\.indeed\.com/f/a/([a-f0-9]{10,16})(?:~~|/)', html)
    if engage_hex:
        return engage_hex.group(1)
    # パターン3: 任意のURLの id= パラメータ（indeed ドメイン内）
    any_id = re.search(r'indeed\.com[^"\'<>\s]*[?&]id=([a-f0-9]{8,20})', html)
    if any_id:
        return any_id.group(1)
    return None

def extract_indeed_engage_urls(html: str) -> list:
    """Indeed通知メールのHTMLからengage.indeed.comトラッキングURLを全て抽出する。
    新形式(base64url)・旧形式(hex)問わず engage.indeed.com/f/a/ URLを返す。
    これらURLはリダイレクトをたどると employers.indeed.com/candidates/view?id=<hex> に到達する。
    """
    if not html:
        return []
    # engage.indeed.com/f/a/<任意の文字列>~~ パターン
    matches = re.findall(r'(https://engage\.indeed\.com/f/a/[A-Za-z0-9_\-]{10,}~~[^\s"\'<>]*)', html)
    return list(dict.fromkeys(matches))  # 重複除去（順序保持）

def extract_phone_number(html: str) -> Optional[str]:
    """メール本文HTMLから電話番号を抽出する。"""
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    # 日本の電話番号パターン（携帯・固定・フリーダイヤル）
    patterns = [
        r'0[789]0[-\s]?\d{4}[-\s]?\d{4}',  # 携帯: 090/080/070
        r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{4}',  # 固定: 03-xxxx-xxxx 等
        r'0120[-\s]?\d{3}[-\s]?\d{3}',  # フリーダイヤル
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    return None

def normalize_phone_number(phone: str) -> str:
    """+81形式を日本国内形式(0XX-XXXX-XXXX)に変換する。"""
    if not phone:
        return phone
    digits = re.sub(r'[\s\-\(\)]', '', phone)
    if digits.startswith('+81'):
        digits = '0' + digits[3:]
    if re.match(r'^0[789]0\d{8}$', digits):  # 携帯 090/080/070
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if re.match(r'^0\d{9}$', digits):  # 固定10桁
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if re.match(r'^0120\d{6}$', digits):  # フリーダイヤル
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return phone

def extract_body_text(html: str, max_chars: int = 500) -> str:
    """メール本文HTMLからプレーンテキストを抽出する（最大max_chars文字）。"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # script/styleタグを除去
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # 連続する空行を1行にまとめる
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    result = "\n".join(lines)
    if len(result) > max_chars:
        result = result[:max_chars] + "…"
    return result

def format_phone_for_slack(phone: str) -> str:
    """Format phone number as a Slack tel: link.
    Converts '+81 80 2478 7813' → '<tel:+818024787813|080-2478-7813>'
    so it becomes a tappable link in Slack mobile.
    """
    if not phone:
        return phone
    # Remove spaces to build the tel URI
    tel_uri = phone.replace(" ", "")
    # Build Japanese local display format: +81 80 XXXX XXXX → 080-XXXX-XXXX
    digits = tel_uri.lstrip("+")
    if digits.startswith("81") and len(digits) >= 11:
        local = "0" + digits[2:]  # 81 → 0
        # Format: 090/080/060 (3 digits) - 4 digits - 4 digits
        if len(local) == 11:
            display = f"{local[:3]}-{local[3:7]}-{local[7:]}"
        elif len(local) == 10:
            display = f"{local[:2]}-{local[2:6]}-{local[6:]}"
        else:
            display = local
    else:
        display = phone
    return f"<tel:{tel_uri}|{display}>"

def format_phone_for_line(phone: str) -> str:
    """Format phone number for LINE tap-to-call.
    Converts '+81 80 2478 7813' → '080-2478-7813'
    LINE automatically turns hyphen-formatted Japanese numbers into tappable links.
    """
    if not phone:
        return phone
    tel_uri = phone.replace(" ", "")
    digits = tel_uri.lstrip("+")
    if digits.startswith("81") and len(digits) >= 11:
        local = "0" + digits[2:]
        if len(local) == 11:
            return f"{local[:3]}-{local[3:7]}-{local[7:]}"
        elif len(local) == 10:
            return f"{local[:2]}-{local[2:6]}-{local[6:]}"
        else:
            return local
    return phone

def shorten_url(url: str) -> str:
    """Shorten URL using TinyURL API. Returns original URL if shortening fails."""
    if not url:
        return url
    try:
        api = "https://tinyurl.com/api-create.php" + "?url=" + quote(url, safe="")
        resp = requests.get(api, timeout=3)
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
    except Exception:
        log("WARNING: shorten_url failed, using original URL")
    return url

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
        r"([^\s \n]+(?:\s[^\s \n]+)?)\s*さん(?:から(?:の)?応募|が応募)",
        r"新しい応募者(?:のお知らせ)?[:：]\s*([^\n\r]+)",
        r"応募者(?:名)?[:：]\s*([^\n\r]+)",
        r"([^\s \n]{1,20})\s*(?:様|さん)(?:\s|$|が|から|の)",
    ]:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # 明らかに名前ではないものを除外（URLや長すぎる文字列）
            if name and len(name) <= 30 and "http" not in name and "@" not in name:
                return name
    return None

# --- Notification Functions ---
def notify_slack_with_retry(
    source: str,
    name: str,
    url: str,
    job_title: Optional[str] = None,
    phone: Optional[str] = None,
    body_text: Optional[str] = None,
    max_retries: int = 3,
    location: Optional[str] = None,
    email_addr: Optional[str] = None,
    answers: Optional[list] = None,
) -> bool:
    """Send notification to Slack with retry logic. Returns True if successful."""
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        log("No Slack Webhook URL")
        return False
    mention_prefix = "<!channel>\n" if not is_test_mode() else ""
    if source == "indeed":
        lines = ["【Indeed 新着応募】"]
        lines.append(f"氏名：{name}")
        if job_title:
            lines.append(f"求人：{job_title}")
        lines.append(f"電話：{format_phone_for_slack(phone) if phone else '未登録'}")
        lines.append(f"住所：{location if location else '未登録'}")
        if email_addr:
            lines.append(f"メール：{email_addr}")
        if url:
            lines.append(f"URL：{shorten_url(url)}")
    else:
        lines = [f"【ジモティー】 【{name}】 さんから応募がありました。"]
        if job_title:
            lines.append(f"求人: {job_title}")
        if phone:
            lines.append(f"電話番号: {format_phone_for_slack(phone)}")
        if location:
            lines.append(f"住所: {location}")
        if email_addr:
            lines.append(f"メール: {email_addr}")
        if answers:
            for ans in answers:
                key = ans.get("questionKey", "")
                val = ans.get("value")
                if val and key:
                    lines.append(f"{key}: {val}")
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

def notify_line_with_retry(
    source: str,
    name: str,
    url: str,
    job_title: Optional[str] = None,
    phone: Optional[str] = None,
    body_text: Optional[str] = None,
    max_retries: int = 3,
    location: Optional[str] = None,
    email_addr: Optional[str] = None,
    answers: Optional[list] = None,
) -> bool:
    """Send notification to LINE with retry logic. Returns True if successful."""
    line_to_id = get_line_to_id()
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return False
    if source == "indeed":
        lines = ["【Indeed 新着応募】"]
        lines.append(f"氏名：{name}")
        if job_title:
            lines.append(f"求人：{job_title}")
        lines.append(f"電話：{format_phone_for_line(phone) if phone else '未登録'}")
        lines.append(f"住所：{location if location else '未登録'}")
        if email_addr:
            lines.append(f"メール：{email_addr}")
        if url:
            lines.append(f"URL：{shorten_url(url)}")
    else:
        lines = [f"【{name}】 さんからジモティーで新着があります。"]
        if job_title:
            lines.append(f"求人: {job_title}")
        if phone:
            lines.append(f"📞 電話番号: {format_phone_for_line(phone)}")
        if location:
            lines.append(f"📍 住所: {location}")
        if email_addr:
            lines.append(f"📧 メール: {email_addr}")
        if answers:
            for ans in answers:
                key = ans.get("questionKey", "")
                val = ans.get("value")
                if val and key:
                    lines.append(f"📝 {key}: {val}")
        if url:
            lines.extend(["", "詳細はこちら:", shorten_url(url)])
    base_message = add_test_prefix("\n".join(lines))
    if is_test_mode():
        # Test mode: no @all mention, plain text
        body = {
            "to": line_to_id,
            "messages": [{"type": "text", "text": base_message}],
        }
    else:
        # Production mode: @all mention via textV2
        substitution = {
            "mention_all": {"type": "mention", "mentionee": {"type": "all"}}
        }
        text_v2 = "{mention_all} " + base_message
        body = {
            "to": line_to_id,
            "messages": [{"type": "textV2", "text": text_v2, "substitution": substitution}],
        }
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    for attempt in range(max_retries):
        try:
            resp = requests.post("https://api.line.me/v2/bot/message/push", json=body, headers=headers, timeout=10)
            log(f"LINE API response: status={resp.status_code}")
            if resp.status_code < 400:
                return True
            log(f"ERROR: LINE notify failed (status={resp.status_code}, body={resp.text}, attempt={attempt + 1}/{max_retries})")
        except requests.exceptions.Timeout:
            log(f"ERROR: LINE notify timeout (attempt={attempt + 1}/{max_retries})")
        except Exception as e:
            log(f"ERROR: LINE notify exception: {e} (attempt={attempt + 1}/{max_retries})")
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
    notify_error_to_slack(f"LINE notify failed after {max_retries} attempts for {name}")
    return False

# --- IMAP Connection ---
@contextmanager
def imap_connection():
    """Context manager for IMAP connection with per-connection timeout (not global).
    Uses timeout= parameter on IMAP4_SSL (Python 3.9+) to avoid modifying global socket state.
    This prevents thread-safety issues when Flask and polling threads run concurrently.
    """
    mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST, timeout=30)
    try:
        mail.login(GMAIL_IMAP_USER, GMAIL_IMAP_PASSWORD)
        mail.select("INBOX", readonly=True)
        yield mail
    finally:
        try:
            mail.close()
            mail.logout()
        except Exception:
            pass

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

def determine_source(subject: str) -> Tuple[Optional[str], Optional[str]]:
    """Determine email source and default URL based on subject."""
    if "新しい応募者のお知らせ" in subject:
        return "indeed", None
    elif "ジモティー" in subject:
        return "jimoty", "https://jmty.jp/web_mail/posts"
    return None, None

def get_unique_id(gm_msgid: Optional[str], msg: email.message.Message) -> Optional[str]:
    """Get unique identifier for email. Prefers X-GM-MSGID, falls back to Message-ID."""
    if gm_msgid:
        return f"gm:{gm_msgid}"
    message_id = msg.get("Message-ID")
    if message_id:
        return f"mid:{message_id}"
    return None

def process_mail_by_uid(
    mail: imaplib.IMAP4_SSL, uid: bytes, processed_ids: Set[str]
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
        return None  # Already processed, skip silently
    subject = decode_header_value(msg.get("Subject", ""))
    from_header = decode_header_value(msg.get("From", ""))
    source, default_url = determine_source(subject)
    if not source:
        log(f"Skip non-target mail: {subject[:50]}...")
        return unique_id  # Mark as processed to avoid re-checking
    html = extract_html(msg)
    url = extract_indeed_url(html) if source == "indeed" else default_url
    # IndeedメールはFrom=「Indeed <noreply@indeed.com>」なので
    # メール本文HTMLから応募者名を取得する。取れなければFromヘッダーの名前を使う。
    if source == "indeed":
        applicant_name = extract_applicant_name_from_html(html)
        if not applicant_name:
            applicant_name = extract_name(from_header)
    else:
        applicant_name = extract_name(from_header)
    # 電話番号・本文テキストを抽出
    phone = extract_phone_number(html)
    # Indeed応募の場合: URLからlegacyIdを抽出してAPIで全詳細を取得
    indeed_location: Optional[str] = None
    indeed_email: Optional[str] = None
    indeed_answers: Optional[list] = None
    if source == "indeed":
        from indeed_fetcher import fetch_all_details, resolve_legacy_id_from_tracking_url, fetch_by_name
        legacy_id = extract_indeed_legacy_id(html)
        if legacy_id:
            log(f"Indeed legacyId found in HTML: {legacy_id}")
        else:
            # HTMLから直接hex IDが取れなかった場合:
            # engage.indeed.com トラッキングURLをたどってhex IDを取得する
            log("Indeed legacyId not found in HTML, trying engage tracking URL redirect...")
            engage_urls = extract_indeed_engage_urls(html)
            log(f"Indeed engage URLs found: {len(engage_urls)}")
            for engage_url in engage_urls:
                legacy_id = resolve_legacy_id_from_tracking_url(engage_url)
                if legacy_id:
                    log(f"Indeed legacyId resolved via engage URL redirect: {legacy_id} (from {engage_url[:60]}...)")
                    break
            if not legacy_id:
                # フォールバック: extract_indeed_url で取得した汎用URLも試みる
                if url and "engage.indeed.com" in url:
                    legacy_id = resolve_legacy_id_from_tracking_url(url)
                    if legacy_id:
                        log(f"Indeed legacyId resolved via fallback URL redirect: {legacy_id}")
                else:
                    log(f"Indeed legacyId not found (no valid engage URL)")
        if legacy_id:
            # legacyIdが取得できた場合、管理画面URLを生成（engage URLより安定・短縮URL用）
            url = f"https://employers.indeed.com/candidates/view?id={legacy_id}"
            details = fetch_all_details(legacy_id)
            if not details:
                # 一時的なAPI障害対策: 3秒後に即リトライ（次サイクル待ちなし）
                log(f"fetch_all_details empty, retrying in 3s...")
                time.sleep(3)
                details = fetch_all_details(legacy_id)
            if details:
                phone = details.get("phone") or phone  # APIの方が正確
                indeed_location = details.get("location")
                indeed_email = details.get("email")
                indeed_answers = details.get("answers") or []
                log(f"Indeed API details: phone={phone}, location={indeed_location}, answers={len(indeed_answers or [])}件")
            else:
                log(f"Indeed API returned no details for legacyId={legacy_id} after retry (CTK expired?)")
                # CTK期限切れ検知 → LINE/Slack で通知（1回のみ）
                try:
                    from indeed_fetcher import is_ctk_expired
                    if is_ctk_expired():
                        notify_ctk_expired()
                except ImportError:
                    pass
            # フォールバック: phoneがない場合も名前検索で補完（GraphQL APIが電話番号を返さないことがある）
            if not phone:
                log(f"Trying name-based search for '{applicant_name}'...")
                name_details = fetch_by_name(applicant_name)
                if name_details:
                    phone = name_details.get("phone") or phone
                    indeed_location = name_details.get("location") or indeed_location
                    indeed_email = name_details.get("email") or indeed_email
                    log(f"Name-search details: phone={phone}, location={indeed_location}, email={indeed_email}")
                else:
                    log(f"Name-search: no match for '{applicant_name}'")
    # ── URL不在チェック（Indeed限定）──────────────────────────────────────
    # URLが取れていない場合は内部リトライを行い、それでもダメならアラートを発報する。
    # 電話番号がない場合は許容（"未登録"表示）だが、URLなしは手動確認が必要。
    if source == "indeed" and not url:
        log(f"URL not found for {applicant_name}, retrying engage URL resolution in 5s...")
        time.sleep(5)
        # engage URLからlegacyIdを再試行
        retry_engage_urls = extract_indeed_engage_urls(html)
        for engage_url in retry_engage_urls:
            from indeed_fetcher import resolve_legacy_id_from_tracking_url as _resolve
            retry_legacy_id = _resolve(engage_url)
            if retry_legacy_id:
                url = f"https://employers.indeed.com/candidates/view?id={retry_legacy_id}"
                log(f"URL resolved on retry: {url[:60]}")
                break
        if not url:
            log(f"URL still not found after retry for {applicant_name} ({unique_id}) — sending alert")
            notify_url_missing(applicant_name, unique_id)
        else:
            log(f"URL obtained on retry for {applicant_name}")
    if phone:
        phone = normalize_phone_number(phone)
    # --- v2: Indeed応募でphone未取得の場合、Coworkに委譲 ---
    if source == "indeed" and not phone:
        signal_id = f"gm:{uid_str}"
        engage_url_for_signal = ""
        try:
            engage_urls_list = extract_indeed_engage_urls(html)
            if engage_urls_list:
                engage_url_for_signal = engage_urls_list[0]
        except Exception:
            pass
        position = subject or ""
        short_url = shorten_url(url) if url else ""
        signal_ok = post_signal_to_slack(signal_id, applicant_name, position, url, engage_url_for_signal, legacy_id or "", short_url)
        if signal_ok:
            record_cas_entry(signal_id, "PENDING", detected_at=datetime.now().astimezone().isoformat(), applicant_name=applicant_name, indeed_url=url or "", short_url=short_url, owner="railway")
            log(f"Signal posted, waiting for Cowork: {signal_id}")
        else:
            send_fallback_notification(applicant_name, url, short_url or url or "")
            record_cas_entry(signal_id, "FALLBACK", detected_at=datetime.now().astimezone().isoformat(), fallback_at=datetime.now().astimezone().isoformat(), applicant_name=applicant_name, indeed_url=url or "", short_url=short_url, owner="railway")
    else:
        log(f"Notify {source}: {applicant_name}, phone={phone}, url={url}, id={unique_id}")
        slack_ok = notify_slack_with_retry(source, applicant_name, url, phone=phone, location=indeed_location, email_addr=indeed_email, answers=indeed_answers)
        line_ok = notify_line_with_retry(source, applicant_name, url, phone=phone, location=indeed_location, email_addr=indeed_email, answers=indeed_answers)
        if not slack_ok:
            log(f"WARNING: Slack notification failed for {applicant_name} ({unique_id})")
        if not line_ok:
            log(f"WARNING: LINE notification failed for {applicant_name} ({unique_id})")
        if not slack_ok and not line_ok:
            phone_str = f"電話: {phone}" if phone else "電話: 未記入"
            notify_error_to_slack(f"【通知失敗】応募者: {applicant_name}\n{phone_str}\nUID: {unique_id}\n\nSlack/LINE通知に失敗しました。Gmailを手動確認してください。")
            log(f"ERROR: All notifications failed for {applicant_name} ({unique_id}) - marked as processed, error alert sent")
    return unique_id

def get_gm_msgid_lightweight(mail: imaplib.IMAP4_SSL, uid: str) -> Optional[str]:
    """Fetch only X-GM-MSGID for a single email (lightweight, no body)."""
    status, data = mail.uid("fetch", uid, "(X-GM-MSGID)")
    if status != "OK":
        return None
    for item in data:
        # IMAP metadata-only fetch returns bytes, not tuples (unlike BODY fetch)
        if isinstance(item, tuple):
            text = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
        elif isinstance(item, bytes):
            text = item.decode()
        else:
            continue
        match = re.search(r"X-GM-MSGID (\d+)", text)
        if match:
            return f"gm:{match.group(1)}"
    return None

def check_mail_with_status() -> bool:
    """Check mailbox for new applications. Returns True if successful, False if quota/error."""
    try:
        global _first_cycle_done

        processed_ids, load_success = load_processed_ids()
        # If file exists but is corrupted, skip processing to prevent mass re-notifications
        if not load_success:
            log("ERROR: Skipping mail check due to corrupted processed IDs file")
            return True  # Not a quota error, don't backoff
        with imap_connection() as mail:
            since_date = (datetime.now() - timedelta(days=SEARCH_DAYS)).strftime("%d-%b-%Y")
            # Use UID SEARCH for stable identifiers
            status, data = mail.uid("search", None, "SINCE", since_date)
            if status != "OK" or not data or not data[0]:
                log(f"ERROR: IMAP UID search failed: status={status}")
                return True  # Not a quota error
            uid_list = data[0].split()
            log(f"Emails in last {SEARCH_DAYS} days: {len(uid_list)}")
            # Phase 1: Quick filter by UID (for emails we've seen before)
            uids_to_check = []
            for uid in uid_list:
                uid_str = uid.decode() if isinstance(uid, bytes) else uid
                if f"uid:{uid_str}" not in processed_ids:
                    uids_to_check.append(uid)
            if not uids_to_check:
                return True  # All emails already processed
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
            # Use merge-on-save to avoid race condition with Flask /manage-processed
            if uids_to_mark:
                log(f"Bootstrapping {len(uids_to_mark)} UIDs for already-processed emails")
                bootstrap_ids = {f"uid:{uid_str}" for uid_str, _ in uids_to_mark}
                if not save_processed_ids_with_merge(bootstrap_ids):
                    log("ERROR: Failed to save bootstrapped UIDs")
                    return True  # Not a quota error

            if truly_new_uids:
                total_new = len(truly_new_uids)

                # === STARTUP PROTECTION: Detect restart with lost state ===
                # 初回サイクルで閾値を超えるメールが見つかった場合、再起動後の重複通知を防ぐ
                # processed_ids の件数に関係なく（部分的なVolume復元も検知）
                with _first_cycle_lock:
                    if not _first_cycle_done and len(truly_new_uids) > STARTUP_NEW_EMAIL_THRESHOLD:
                        log(f"STARTUP PROTECTION: {len(truly_new_uids)} new emails found on first cycle "
                            f"(threshold={STARTUP_NEW_EMAIL_THRESHOLD}, existing processed_ids={len(processed_ids)}).")
                        log("Silently marking as processed to prevent re-notification on restart...")
                        # 全件を軽量取得してgm:IDのみ記録（通知なし）
                        startup_ids: Set[str] = set()
                        for uid in truly_new_uids:
                            uid_str = uid.decode() if isinstance(uid, bytes) else uid
                            gm_id = get_gm_msgid_lightweight(mail, uid_str)
                            if gm_id:
                                startup_ids.add(gm_id)
                                startup_ids.add(f"uid:{uid_str}")
                        save_processed_ids_with_merge(startup_ids)
                        log(f"STARTUP PROTECTION: Silently marked {len(truly_new_uids)} emails. Next cycle processes only truly new emails.")
                        _first_cycle_done = True
                        return True
                    _first_cycle_done = True

                # QUOTA ERROR対策: 1サイクルで処理するメール数を制限する
                batch = truly_new_uids[:MAX_EMAILS_PER_CYCLE]
                if total_new > MAX_EMAILS_PER_CYCLE:
                    log(f"Truly new emails to process: {total_new} (processing {MAX_EMAILS_PER_CYCLE} this cycle, {total_new - MAX_EMAILS_PER_CYCLE} deferred)")
                else:
                    log(f"Truly new emails to process: {total_new}")
                # Phase 3: Full processing for truly new emails only (batch limited)
                # Collect new IDs for this cycle, then merge-save once per batch (not per email)
                # This minimizes lock contention while preventing race with Flask endpoints
                new_ids_this_cycle: Set[str] = set()
                for uid in batch:
                    uid_str = uid.decode() if isinstance(uid, bytes) else uid
                    unique_id = process_mail_by_uid(mail, uid, processed_ids | new_ids_this_cycle)
                    if unique_id:
                        new_ids_this_cycle.add(unique_id)
                        new_ids_this_cycle.add(f"uid:{uid_str}")
                # Save all new IDs at once with atomic merge (prevents race with Flask)
                if new_ids_this_cycle:
                    if not save_processed_ids_with_merge(new_ids_this_cycle):
                        log("ERROR: Failed to save processed IDs after batch")
                        return True  # Not a quota error
            return True  # Success
    except imaplib.IMAP4.abort as e:
        error_msg = str(e)
        if "OVERQUOTA" in error_msg:
            log(f"QUOTA ERROR: {error_msg}")
            return False  # Quota error, trigger backoff
        log(f"ERROR: IMAP abort: {e}")
        return False  # Other IMAP error, also backoff
    except (imaplib.IMAP4.error, socket.timeout, socket.gaierror) as e:
        log(f"ERROR: IMAP/socket error: {e}")
        notify_error_to_slack(f"Gmail IMAP connection error: {e}")
        return True  # Not necessarily a quota error
    except Exception as e:
        log(f"ERROR: {e}")
        # Check if it's a quota-related error
        if "OVERQUOTA" in str(e):
            return False  # Quota error, trigger backoff
        notify_error_to_slack(f"Gmail polling error: {e}")
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

# --- Flask Webhook Server (Cowork LINE compatible) ---

def load_cas_store() -> dict:
    try:
        if os.path.exists(CAS_STATE_FILE):
            with open(CAS_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log(f"ERROR: Failed to load CAS store: {e}")
    return {}

def save_cas_store(store: dict) -> bool:
    try:
        with open(CAS_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        log(f"ERROR: Failed to save CAS store: {e}")
        return False

def record_cas_entry(signal_id: str, status: str, **kwargs) -> None:
    with _cas_store_lock:
        store = load_cas_store()
        entry = store.get(signal_id, {})
        entry["status"] = status
        for k, v in kwargs.items():
            entry[k] = v
        store[signal_id] = entry
        save_cas_store(store)

def get_cas_entry(signal_id: str) -> Optional[dict]:
    with _cas_store_lock:
        store = load_cas_store()
        return store.get(signal_id)

def post_signal_to_slack(signal_id, applicant_name, position, indeed_url, engage_url, legacy_id, short_url=""):
    channel = COWORK_QUEUE_CHANNEL
    bot_token = SLACK_BOT_TOKEN
    if not bot_token:
        log("ERROR: SLACK_BOT_TOKEN not set, cannot post signal")
        return False
    jst = datetime.now().astimezone()
    payload = {"type": "indeed_application", "id": signal_id, "applicant_name": applicant_name, "position": position or "", "indeed_url": indeed_url or "", "engage_url": engage_url or "", "legacy_id": legacy_id or "", "short_url": short_url, "detected_at": jst.isoformat(), "status": "PENDING"}
    for attempt in range(3):
        try:
            resp = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {bot_token}"}, json={"channel": channel, "text": json.dumps(payload, ensure_ascii=False)}, timeout=10)
            if resp.ok and resp.json().get("ok"):
                log(f"Signal posted to Slack: {signal_id}")
                return True
            else:
                log(f"Signal post failed (attempt {attempt+1}): {resp.text[:200]}")
        except Exception as e:
            log(f"Signal post attempt {attempt+1} error: {e}")
            time.sleep(2)
    log(f"Signal post failed after 3 attempts: {signal_id}")
    return False


def send_fallback_notification(applicant_name, indeed_url, short_url=None):
    url = short_url or indeed_url or ""
    log(f"Sending fallback notification for {applicant_name}")
    mention = "<!channel>\n" if not is_test_mode() else ""
    slack_text = f"{mention}【Indeed 新着応募（速報）】\n氏名：{applicant_name}\n電話：⚠ 手動確認が必要\nURL：{url}\n※ Indeed管理画面で電話番号を確認してください"
    webhook_url = get_slack_webhook_url()
    if webhook_url:
        try:
            resp = requests.post(webhook_url, json={"text": slack_text}, timeout=10)
            if resp.status_code < 400:
                log(f"Fallback Slack sent for {applicant_name}")
            else:
                log(f"ERROR: Fallback Slack failed: {resp.status_code}")
        except Exception as e:
            log(f"ERROR: Fallback Slack error: {e}")
    line_to_id = get_line_to_id()
    if LINE_CHANNEL_ACCESS_TOKEN and line_to_id:
        line_text = f"@all\n【Indeed 新着応募（速報）】\n氏名：{applicant_name}\n電話：⚠ 手動確認が必要\nURL：{url}\n※ Indeed管理画面で電話番号を確認してください"
        try:
            resp = requests.post("https://api.line.me/v2/bot/message/push", json={"to": line_to_id, "messages": [{"type": "text", "text": line_text}]}, headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}, timeout=10)
            if resp.status_code < 400:
                log(f"Fallback LINE sent for {applicant_name}")
            else:
                log(f"ERROR: Fallback LINE failed: {resp.status_code}")
        except Exception as e:
            log(f"ERROR: Fallback LINE error: {e}")

def check_fallback_timers():
    now = datetime.now().astimezone()
    timeout = timedelta(seconds=FALLBACK_TIMEOUT_SECONDS)
    with _cas_store_lock:
        store = load_cas_store()
        changed = False
        for signal_id, entry in list(store.items()):
            status = entry.get("status")
            if status not in ("PENDING", "LOCKED"):
                continue
            detected_str = entry.get("detected_at", "")
            if not detected_str:
                continue
            try:
                detected_at = datetime.fromisoformat(detected_str)
            except (ValueError, TypeError):
                continue
            if now - detected_at > timeout:
                entry["status"] = "FALLBACK"
                entry["fallback_at"] = now.isoformat()
                entry["owner"] = "railway"
                store[signal_id] = entry
                changed = True
                send_fallback_notification(applicant_name=entry.get("applicant_name", "不明"), indeed_url=entry.get("indeed_url", ""), short_url=entry.get("short_url", ""))
                log(f"Fallback triggered for {signal_id} (status was {status})")
        if changed:
            save_cas_store(store)

def start_fallback_checker():
    while True:
        try:
            check_fallback_timers()
        except Exception as e:
            log(f"Fallback checker error: {e}")
        time.sleep(FALLBACK_CHECK_INTERVAL)

flask_app = Flask(__name__)

@flask_app.after_request
def add_cors(response):
    allowed_origins = {"https://employers.indeed.com", "https://cowork.anthropic.com", "https://claude.ai"}
    origin = flask_request.headers.get("Origin", "")
    if origin in allowed_origins:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Cowork-Token"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS, GET"
    return response

@flask_app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"})

@flask_app.route("/api/cas", methods=["POST", "OPTIONS"])
def api_cas():
    if flask_request.method == "OPTIONS":
        return "", 200
    token = os.environ.get("COWORK_WEBHOOK_TOKEN", "")
    if not token:
        return jsonify({"ok": False, "error": "server_misconfigured"}), 500
    # Support both Authorization: Bearer header and ?token= query parameter
    auth_header = flask_request.headers.get("Authorization", "")
    query_token = flask_request.args.get("token", "")
    if auth_header.startswith("Bearer "):
        provided_token = auth_header.replace("Bearer ", "")
    elif query_token:
        provided_token = query_token
    else:
        provided_token = ""
    if not hmac.compare_digest(provided_token, token):
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = flask_request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "invalid_json"}), 400
    signal_id = data.get("id")
    expected_from = data.get("from")
    target_to = data.get("to")
    owner = data.get("owner", "unknown")
    if not all([signal_id, expected_from, target_to]):
        return jsonify({"ok": False, "error": "missing_fields"}), 400
    valid_transitions = {("PENDING", "LOCKED"), ("LOCKED", "NOTIFIED"), ("LOCKED", "FALLBACK"), ("PENDING", "FALLBACK")}
    if (expected_from, target_to) not in valid_transitions:
        return jsonify({"ok": False, "error": "invalid_transition"}), 400
    jst_now = datetime.now().astimezone().isoformat()
    with _cas_store_lock:
        store = load_cas_store()
        entry = store.get(signal_id, {})
        current_status = entry.get("status", "PENDING")
        if current_status != expected_from:
            return jsonify({"ok": False, "error": "state_mismatch", "id": signal_id, "expected": expected_from, "actual": current_status}), 409
        entry["status"] = target_to
        entry["owner"] = owner
        if target_to == "LOCKED":
            entry["locked_at"] = jst_now
        elif target_to == "NOTIFIED":
            entry["notified_at"] = jst_now
        elif target_to == "FALLBACK":
            entry["fallback_at"] = jst_now
        store[signal_id] = entry
        save_cas_store(store)
    log(f"CAS: {signal_id} {expected_from} -> {target_to} (owner={owner})")
    return jsonify({"ok": True, "id": signal_id, "previous": expected_from, "current": target_to, "locked_at": entry.get("locked_at", "")}), 200


@flask_app.route("/test-ctk", methods=["GET"])
def test_ctk():
    """診断用: CTKの有効性とIndeed API接続をテストする。legacyIdを指定すると詳細も取得。"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    legacy_id = flask_request.args.get("id", "")
    from indeed_fetcher import get_ctk, fetch_all_details
    ctk = get_ctk()
    result = {
        "ctk_set": bool(ctk),
        "ctk_prefix": ctk[:8] + "..." if len(ctk) > 8 else ctk,
    }
    if legacy_id:
        details = fetch_all_details(legacy_id)
        result["legacy_id"] = legacy_id
        result["details_empty"] = not bool(details)
        result["phone"] = details.get("phone") if details else None
        result["name"] = details.get("name") if details else None
        result["location"] = details.get("location") if details else None
        result["email"] = details.get("email") if details else None
    return jsonify(result)

# --- CTK更新フォーム（モバイル対応） ---
_CTK_UPDATE_FORM_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>Indeed CTK 更新</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           padding: 24px 20px; max-width: 520px; margin: 0 auto; background: #f8f9fa; color: #222; }
    h1 { font-size: 22px; margin-bottom: 6px; }
    .sub { color: #666; font-size: 14px; margin-bottom: 20px; }
    textarea { width: 100%; height: 130px; font-size: 13px; padding: 12px;
               border: 2px solid #d1d5db; border-radius: 10px; resize: vertical;
               font-family: monospace; background: #fff; }
    textarea:focus { outline: none; border-color: #2563eb; }
    button { width: 100%; padding: 16px; background: #2563eb; color: #fff;
             border: none; border-radius: 10px; font-size: 17px; font-weight: bold;
             margin-top: 14px; cursor: pointer; letter-spacing: 0.5px; }
    button:active { background: #1d4ed8; }
    .howto { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px;
             padding: 14px 16px; margin-top: 20px; font-size: 13px; color: #555; }
    .howto b { color: #222; display: block; margin-bottom: 6px; }
    .howto ol { margin: 0; padding-left: 18px; line-height: 1.8; }
  </style>
</head>
<body>
  <h1>⚙️ Indeed CTK 更新</h1>
  <p class="sub">新しいCTK値を貼り付けて「更新する」を押してください。再デプロイ不要で即反映されます。</p>
  <form method="POST">
    <textarea name="ctk" placeholder="CTK値をここに貼り付け..." autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"></textarea>
    <button type="submit">✅ 更新する</button>
  </form>
  <div class="howto">
    <b>📋 CTKの取得手順（PCのChromeで）</b>
    <ol>
      <li>jp.indeed.com にログイン</li>
      <li>F12（またはCtrl+Shift+I）→ Application タブ → Cookies → jp.indeed.com</li>
      <li>「CTK」の値をコピー</li>
      <li>このページに貼り付けて送信</li>
    </ol>
  </div>
  <div class="howto" style="margin-top:10px;">
    <b>📱 スマホの場合</b>
    <ol>
      <li>PCのChromeで上の手順でCTKを取得</li>
      <li>自分にメール等でCTK値を送る</li>
      <li>スマホでこのページを開き、貼り付けて送信</li>
    </ol>
    <p style="margin:8px 0 0;color:#888;font-size:12px;">※ スマホのブラウザではCookieを直接確認できないため、PCでの取得が必要です。</p>
  </div>
  <script>
    (function() {
      var params = new URLSearchParams(window.location.search);
      var ctk = params.get('ctk');
      if (ctk) {
        document.querySelector('textarea[name="ctk"]').value = decodeURIComponent(ctk);
      }
    })();
  </script>
</body>
</html>"""

_CTK_UPDATE_SUCCESS_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CTK更新完了</title>
  <style>
    body { font-family: -apple-system, sans-serif; padding: 40px 24px;
           text-align: center; max-width: 400px; margin: 0 auto; }
    .icon { font-size: 60px; margin-bottom: 16px; }
    h1 { font-size: 24px; color: #16a34a; margin-bottom: 10px; }
    p { color: #555; font-size: 15px; line-height: 1.6; }
  </style>
</head>
<body>
  <div class="icon">✅</div>
  <h1>CTK更新完了</h1>
  <p>Indeed APIの認証が再開されました。<br>次回の応募通知から電話番号・住所が届きます。</p>
</body>
</html>"""

@flask_app.route("/update-ctk-setup", methods=["GET"])
def update_ctk_setup():
    """ブックマークレット設定ページ。ワンタップCTK更新の初回セットアップ用。"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    # ブックマークレットJS（jp.indeed.comのCTKを自動読み取りしてPOST送信）
    post_url = f"{RAILWAY_SERVICE_URL}/update-ctk?token={COWORK_WEBHOOK_TOKEN}"
    manual_url = f"{RAILWAY_SERVICE_URL}/update-ctk?token={COWORK_WEBHOOK_TOKEN}"
    bookmarklet_js = (
        "javascript:(function(){{"
        "var v='';"
        "var cookies=document.cookie.split(';');"
        "for(var i=0;i<cookies.length;i++){{"
        "var t=cookies[i].trim();"
        "if(t.toUpperCase().startsWith('CTK=')){{v=t.substring(4);break;}}"
        "}};"
        "if(!v&&window.mosaic&&window.mosaic.mos_ctk){{v=window.mosaic.mos_ctk;}};"
        "if(!v){{"
        "try{{var m=document.querySelector('meta[name=indeed-ctk]');if(m){{v=m.content;}}}}catch(e){{}}"
        "}};"
        "if(!v){{window.location.href='{manual_url}';return;}}"
        "window.location.href='{manual_url}'+'&ctk='+encodeURIComponent(v);"
        "}})();"
    ).format(post_url=post_url, manual_url=manual_url)
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>CTK更新 ワンタップ設定</title>
  <style>
    *{{box-sizing:border-box;}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:20px;max-width:540px;margin:0 auto;background:#f8f9fa;color:#222;}}
    h1{{font-size:21px;margin-bottom:4px;}}
    .sub{{color:#666;font-size:14px;margin-bottom:24px;}}
    .step{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px 18px;margin-bottom:14px;}}
    .step-num{{display:inline-block;background:#2563eb;color:#fff;border-radius:50%;width:26px;height:26px;text-align:center;line-height:26px;font-size:13px;font-weight:bold;margin-right:8px;}}
    .step-title{{font-size:16px;font-weight:bold;}}
    .step-body{{color:#555;font-size:14px;margin-top:8px;line-height:1.7;}}
    .bm-link{{display:block;background:#f59e0b;color:#fff;text-align:center;padding:14px;border-radius:10px;font-size:17px;font-weight:bold;text-decoration:none;margin-top:10px;}}
    .bm-link:active{{background:#d97706;}}
    .after{{background:#dcfce7;border:1px solid #86efac;border-radius:12px;padding:16px 18px;margin-top:6px;}}
    .after-title{{font-size:15px;font-weight:bold;color:#16a34a;}}
    .after-body{{color:#166534;font-size:14px;margin-top:6px;line-height:1.7;}}
    .note{{font-size:12px;color:#9ca3af;margin-top:20px;text-align:center;}}
  </style>
</head>
<body>
  <h1>⚡ CTK更新 ワンタップ設定</h1>
  <p class="sub">一度設定すれば、次回からボタン1つでCTKを更新できます。</p>

  <div class="step">
    <span class="step-num">1</span><span class="step-title">ブックマークレットを保存する</span>
    <div class="step-body">
      下のボタンを<b>長押し（または右クリック）→「リンクをブックマークに追加」</b>で保存してください。<br>
      名前は <b>「CTK更新」</b> にしておくと便利です。
      <a class="bm-link" href="{bookmarklet_js}">⭐ CTK更新（ブックマーク用ボタン）</a>
    </div>
  </div>

  <div class="step">
    <span class="step-num">2</span><span class="step-title">CTKが切れたら…</span>
    <div class="step-body">
      ① Chromeで <b>jp.indeed.com</b> を開く（ログイン済みであればOK）<br>
      ② ブラウザの <b>☆ お気に入り → 「CTK更新」</b> をタップ<br>
      ③ CTK値が自動入力されたページが開く<br>
      ④ 「更新する」ボタンを押して完了！<br><br>
      <span style="color:#b45309;font-size:13px;">⚠ 自動取得できない場合は手動入力フォームに転送されます。<br>
      その場合はPCのChromeで <b>F12 → Application → Cookies → CTK</b> の値をコピーして貼り付けてください。</span>
    </div>
  </div>

  <div class="after">
    <div class="after-title">✅ 設定完了後の手順はこれだけ</div>
    <div class="after-body">
      jp.indeed.com を開く → お気に入りから「CTK更新」をタップ → CTK自動入力 → 「更新する」を押す<br>
      <span style="font-size:13px;color:#166534;">（自動取得できない場合は手動入力フォームで対応可）</span>
    </div>
  </div>

  <p class="note">このページのURLは保管してください。次回のセットアップ時に必要です。</p>
</body>
</html>"""
    return html


@flask_app.route("/update-ctk", methods=["GET", "POST"])
def update_ctk_endpoint():
    """CTK更新フォーム（モバイル対応）。COWORK_WEBHOOK_TOKENで認証。"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401

    if flask_request.method == "GET":
        return _CTK_UPDATE_FORM_HTML

    # POST: CTKを更新してフラグをリセット
    new_ctk = flask_request.form.get("ctk", "").strip()
    if not new_ctk:
        return "CTK is required", 400
    try:
        from indeed_fetcher import reset_ctk_expired, _CTK_FILE
        with open(_CTK_FILE, "w", encoding="utf-8") as f:
            f.write(new_ctk)
        reset_ctk_expired()
        log(f"CTK updated via web form (length={len(new_ctk)})")
    except Exception as e:
        log(f"ERROR: CTK update via web form failed: {e}")
        return f"Error: {e}", 500
    # CTK期限切れ通知フラグもリセット（次回期限切れ時に再通知できるように）
    global _ctk_expired_notified
    with _ctk_expired_notified_lock:
        _ctk_expired_notified = False
    log("CTK flags reset. System will resume normal operation on next poll.")
    return _CTK_UPDATE_SUCCESS_HTML


# ── セッションCookie更新（電話番号取得に必要） ──────────────────────────────
@flask_app.route("/update-session-setup", methods=["GET"])
def update_session_setup():
    """セッションCookieブックマークレット設定ページ。
    employers.indeed.com 上で実行するとCookie一式をRailwayに送信する。
    """
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    post_url = f"{RAILWAY_SERVICE_URL}/update-session?token={COWORK_WEBHOOK_TOKEN}"
    # ブックマークレットJS: employers.indeed.comで実行 → 全Cookieを送信
    bookmarklet_js = (
        "javascript:(function(){{"
        "var c=document.cookie;"
        "if(!c){{alert('Cookieが取得できません。employers.indeed.comで実行してください。');return;}}"
        "var url='{post_url}&cookies='+encodeURIComponent(c);"
        "window.location.href=url;"
        "}})();"
    ).format(post_url=post_url)
    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>セッション更新 設定</title>
  <style>
    *{{box-sizing:border-box;}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:20px;max-width:540px;margin:0 auto;background:#f8f9fa;color:#222;}}
    h1{{font-size:21px;margin-bottom:4px;}}
    .sub{{color:#666;font-size:14px;margin-bottom:24px;}}
    .step{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px 18px;margin-bottom:14px;}}
    .step-num{{display:inline-block;background:#059669;color:#fff;border-radius:50%;width:26px;height:26px;text-align:center;line-height:26px;font-size:13px;font-weight:bold;margin-right:8px;}}
    .step-title{{font-size:16px;font-weight:bold;}}
    .step-body{{color:#555;font-size:14px;margin-top:8px;line-height:1.7;}}
    .bm-link{{display:block;background:#059669;color:#fff;text-align:center;padding:14px;border-radius:10px;font-size:17px;font-weight:bold;text-decoration:none;margin-top:10px;}}
    .bm-link:active{{background:#047857;}}
    .warn{{background:#fef3c7;border:1px solid #fcd34d;border-radius:10px;padding:12px 16px;font-size:13px;color:#92400e;margin-bottom:16px;}}
    .after{{background:#dcfce7;border:1px solid #86efac;border-radius:12px;padding:16px 18px;margin-top:6px;}}
    .after-title{{font-size:15px;font-weight:bold;color:#16a34a;}}
    .after-body{{color:#166534;font-size:14px;margin-top:6px;line-height:1.7;}}
  </style>
</head>
<body>
  <h1>📱 セッション更新 設定</h1>
  <p class="sub">電話番号を取得するためのCookieをRailwayに登録します。</p>

  <div class="warn">
    ⚠️ <b>employers.indeed.com</b> を開いた状態でこのブックマークを実行してください。<br>
    jp.indeed.comでは動作しません。
  </div>

  <div class="step">
    <span class="step-num">1</span><span class="step-title">ブックマークレットを保存する</span>
    <div class="step-body">
      下のボタンを<b>長押し（または右クリック）→「リンクをブックマークに追加」</b>で保存してください。<br>
      名前は <b>「Indeed セッション更新」</b> にしてください。
      <a class="bm-link" href="{bookmarklet_js}">🔑 Indeed セッション更新（ブックマーク用）</a>
    </div>
  </div>

  <div class="step">
    <span class="step-num">2</span><span class="step-title">定期的に実行する（月1〜2回）</span>
    <div class="step-body">
      ① Chromeで <b>employers.indeed.com/candidates</b> を開く（ログイン済みであればOK）<br>
      ② ブラウザの <b>☆ お気に入り → 「Indeed セッション更新」</b> をタップ<br>
      ③ 「セッション更新完了」と表示されれば完了！<br><br>
      <span style="color:#b45309;font-size:13px;">
        💡 電話番号が「未登録」と表示される場合や、URLアラートが来た場合に実行してください。
      </span>
    </div>
  </div>

  <div class="after">
    <div class="after-title">✅ 設定後の効果</div>
    <div class="after-body">
      応募通知に電話番号・住所が含まれるようになります。<br>
      セッションが切れた場合は同じ手順で再実行してください（月1〜2回程度）。
    </div>
  </div>
</body>
</html>"""
    return html


@flask_app.route("/update-session", methods=["GET"])
def update_session_endpoint():
    """セッションCookieを受け取って保存する。ブックマークレットから呼ばれる。"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    cookies_str = flask_request.args.get("cookies", "").strip()
    if not cookies_str:
        return "cookies parameter is required", 400
    try:
        from indeed_fetcher import save_session_cookies
        save_session_cookies(cookies_str)
        log(f"Session cookies updated via bookmarklet (length={len(cookies_str)})")
        # CTK expired フラグもリセット（セッション更新で一緒にCTKも更新される可能性あり）
        try:
            from indeed_fetcher import reset_ctk_expired
            reset_ctk_expired()
        except Exception:
            pass
        global _ctk_expired_notified
        with _ctk_expired_notified_lock:
            _ctk_expired_notified = False
    except Exception as e:
        log(f"ERROR: Session cookies update failed: {e}")
        return f"Error: {e}", 500
    return """<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>セッション更新完了</title>
<style>body{{font-family:-apple-system,sans-serif;padding:40px 24px;text-align:center;max-width:400px;margin:0 auto;}}.icon{{font-size:64px;margin-bottom:16px;}}</style>
</head><body>
<div class="icon">✅</div>
<h1>セッション更新完了</h1>
<p>Cookieが登録されました。<br>次回の応募通知から電話番号が届きます。</p>
</body></html>"""


@flask_app.route("/send-setup-msg", methods=["GET"])
def send_setup_msg():
    """LINEグループにCTKセットアップURLを送信する（初回設定用・GET呼び出し可）。"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    setup_url = f"{RAILWAY_SERVICE_URL}/update-ctk-setup?token={COWORK_WEBHOOK_TOKEN}"
    msg = (
        "【CTK更新 初回設定のお願い】\n\n"
        "IndeedのCTKが期限切れになったとき、\n"
        "ワンタップで更新できるブックマークレットを設定してください。\n\n"
        "① 下のURLをChromeで開く\n"
        "② 表示された手順に従ってブックマークを追加\n"
        "③ 次回CTK切れ通知が来たらブックマークをタップするだけ\n\n"
        f"{setup_url}"
    )
    line_to_id = get_line_to_id()  # is_test_mode() 経由で統一（直接 MODE 参照を廃止）
    if not line_to_id or not LINE_CHANNEL_ACCESS_TOKEN:
        return "LINE not configured", 500
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            json={"to": line_to_id, "messages": [{"type": "text", "text": msg}]},
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            timeout=10,
        )
        log(f"[send-setup-msg] LINE status={resp.status_code}")
        if resp.status_code < 400:
            return "✅ LINEに送信しました", 200
        return f"LINE API error: {resp.status_code} {resp.text}", 500
    except Exception as e:
        log(f"[send-setup-msg] error: {e}")
        return f"Error: {e}", 500


@flask_app.route("/send-test-personal", methods=["GET"])
def send_test_personal():
    """個人LINEにテストメッセージを送信する（GETで呼び出し可）"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return "Unauthorized", 401
    personal_id = LINE_TO_ID_PERSONAL
    if not personal_id or not LINE_CHANNEL_ACCESS_TOKEN:
        return "LINE_TO_ID_PERSONAL not configured", 500
    msg = flask_request.args.get("msg", "【テスト】個人LINE通知テストです。このメッセージが届いていれば設定成功です。")
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            json={"to": personal_id, "messages": [{"type": "text", "text": msg}]},
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"},
            timeout=10,
        )
        log(f"[send-test-personal] LINE status={resp.status_code}")
        if resp.status_code < 400:
            return "✅ 個人LINEに送信しました", 200
        return f"LINE API error: {resp.status_code} {resp.text}", 500
    except Exception as e:
        log(f"[send-test-personal] error: {e}")
        return f"Error: {e}", 500


@flask_app.route("/notify-line", methods=["POST", "OPTIONS"])
def notify_line_webhook():
    if flask_request.method == "OPTIONS":
        return "", 204
    if not COWORK_WEBHOOK_TOKEN:
        log("ERROR: COWORK_WEBHOOK_TOKEN not configured - rejecting all requests for security")
        return jsonify({"error": "Service not configured"}), 503
    if not hmac.compare_digest(flask_request.headers.get("X-Cowork-Token", ""), COWORK_WEBHOOK_TOKEN):
        return jsonify({"error": "Unauthorized"}), 401
    data = flask_request.get_json(force=True) or {}
    name = data.get("name", "")
    phone = data.get("phone") or None
    email_addr = data.get("email") or None
    address = data.get("address") or None
    url = data.get("url") or None
    log(f"[webhook] notify-line: name={name}, phone={phone}, email={email_addr}, url={'yes' if url else 'no'}")
    ok = notify_line_with_retry("indeed", name, "", phone=phone, email_addr=email_addr, location=address, url=url)
    return jsonify({"ok": ok})

@flask_app.route("/manage-processed", methods=["GET", "POST"])
def manage_processed():
    """Utility endpoint to view/delete processed IDs. Auth via COWORK_WEBHOOK_TOKEN."""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or not hmac.compare_digest(token, COWORK_WEBHOOK_TOKEN):
        return jsonify({"error": "unauthorized"}), 403

    action = flask_request.args.get("action", "info")

    if action == "info":
        processed_ids, ok = load_processed_ids()
        return jsonify({"count": len(processed_ids), "ok": ok})

    elif action == "search":
        q = flask_request.args.get("q", "")
        processed_ids, ok = load_processed_ids()
        if not q:
            return jsonify({"error": "q parameter required"}), 400
        matches = [pid for pid in processed_ids if q in pid]
        return jsonify({"matches": sorted(matches), "count": len(matches)})

    elif action == "list_recent":
        try:
            count = int(flask_request.args.get("count", "20"))
        except (ValueError, TypeError):
            count = 20
        processed_ids, ok = load_processed_ids()
        sorted_ids = sorted(processed_ids, reverse=True)
        return jsonify({"ids": sorted_ids[:count], "total": len(processed_ids)})

    elif action == "delete":
        ids_param = flask_request.args.get("ids", "")
        if not ids_param:
            return jsonify({"error": "ids parameter required (comma-separated)"}), 400
        ids_to_delete = set(ids_param.split(","))
        with _processed_ids_lock:
            processed_ids, ok = load_processed_ids()
            if not ok:
                return jsonify({"error": "could not load processed_ids"}), 500
            found = ids_to_delete & processed_ids
            not_found = ids_to_delete - processed_ids
            processed_ids -= found
            if found:
                save_processed_ids(processed_ids)
                log(f"manage-processed: deleted {len(found)} IDs: {found}")
            return jsonify({
                "deleted": sorted(found),
                "not_found": sorted(not_found),
                "remaining_count": len(processed_ids)
            })

    return jsonify({"error": f"unknown action: {action}"}), 400


def run_flask_server() -> None:
    port = int(os.getenv("PORT", "8080"))
    log(f"Starting Flask webhook server on port {port}")
    # threaded=True: allows concurrent requests (health check not blocked by long-running endpoints)
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)

# --- Main loop ---
def main() -> None:
    """Main polling loop with exponential backoff for quota errors."""
    # Start Flask webhook server in background thread
    flask_thread = Thread(target=run_flask_server, daemon=True)
    flask_thread.start()
    fb_thread = Thread(target=start_fallback_checker, daemon=True)
    fb_thread.start()
    log(f"Fallback checker started (timeout={FALLBACK_TIMEOUT_SECONDS}s, interval={FALLBACK_CHECK_INTERVAL}s)")
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")
    log(f"MODE={MODE}, SEARCH_DAYS={SEARCH_DAYS}, MAX_BACKOFF_SECONDS={MAX_BACKOFF_SECONDS}, MAX_EMAILS_PER_CYCLE={MAX_EMAILS_PER_CYCLE}")
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

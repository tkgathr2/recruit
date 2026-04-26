"""Gmail polling service for Indeed/Jimoty job application notifications."""
import imaplib
import email
from email.header import decode_header
import os
import socket
import time
import json
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Set, Tuple
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup
from flask import Flask, request as flask_request, jsonify
from threading import Thread, RLock
from datetime import datetime, timedelta

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
COWORK_WEBHOOK_TOKEN = os.getenv("COWORK_WEBHOOK_TOKEN", "")
_processed_ids_lock = RLock()  # Thread-safe access to processed_ids
LOG_DIR = os.getenv("LOG_DIR", "/tmp")
SLACK_ERROR_WEBHOOK_URL = os.getenv("SLACK_ERROR_WEBHOOK_URL")

# --- Processed IDs file for duplicate prevention ---
PROCESSED_IDS_FILE = os.getenv("PROCESSED_IDS_FILE", os.path.join(LOG_DIR, "processed_ids.json"))

# --- Mention IDs (generic slots: set as many as needed) ---
SLACK_MENTION_ID_1 = os.getenv("SLACK_MENTION_ID_1")
SLACK_MENTION_ID_2 = os.getenv("SLACK_MENTION_ID_2")
LINE_MENTION_ID_1 = os.getenv("LINE_MENTION_ID_1")
LINE_MENTION_ID_2 = os.getenv("LINE_MENTION_ID_2")

# --- Polling Interval ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))  # デフォルト60秒（Gmail API制限対策）
MAX_BACKOFF_SECONDS = int(os.getenv("MAX_BACKOFF_SECONDS", "900"))  # 最大15分のバックオフ

# --- Search window for emails (days) ---
SEARCH_DAYS = int(os.getenv("SEARCH_DAYS", "1"))  # デフォルト1日間（Gmail API制限対策）

# --- Batch limit per cycle (QUOTA ERROR対策) ---
MAX_EMAILS_PER_CYCLE = int(os.getenv("MAX_EMAILS_PER_CYCLE", "10"))  # 1サイクルで処理する最大メール数


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
        persistent_ids = [id for id in processed_ids if not id.startswith("uid:")]
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


def notify_error_to_slack(message: str) -> None:
    """重大なエラーを Slack Webhook に通知する"""
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


def extract_indeed_legacy_id(html: str) -> Optional[str]:
    """Indeed通知メールのHTMLからlegacyId（hex）を抽出する。

    Indeed通知メールには以下のURLパターンが含まれる:
    - https://employers.indeed.com/candidates/view?id=<legacyId>
    - https://engage.indeed.com/f/a/<legacyId>~~/...  (旧形式: hex)
    - https://engage.indeed.com/f/a/<base64url>~~...  (新形式: base64url 22文字)
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
        r'0[789]0[-\s]?\d{4}[-\s]?\d{4}',   # 携帯: 090/080/070
        r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{4}', # 固定: 03-xxxx-xxxx 等
        r'0120[-\s]?\d{3}[-\s]?\d{3}',        # フリーダイヤル
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
    if re.match(r'^0[789]0\d{8}$', digits):   # 携帯 090/080/070
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if re.match(r'^0\d{9}$', digits):           # 固定10桁
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if re.match(r'^0120\d{6}$', digits):        # フリーダイヤル
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
        resp = requests.get(api, timeout=5)
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


# --- Notification Functions ---
def notify_slack_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, phone: Optional[str] = None, body_text: Optional[str] = None, max_retries: int = 3, location: Optional[str] = None, email_addr: Optional[str] = None, answers: Optional[list] = None) -> bool:
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


def notify_line_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, phone: Optional[str] = None, body_text: Optional[str] = None, max_retries: int = 3, location: Optional[str] = None, email_addr: Optional[str] = None, answers: Optional[list] = None) -> bool:
    """Send notification to LINE with retry logic. Returns True if successful."""
    line_to_id = get_line_to_id()
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return False
    title = "Indeedに応募がありました。" if source == "indeed" else "ジモティーで新着があります。"
    lines = [f"【{name}】 さんから{title}"]
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
        # Force LINE to open URL in external browser (Chrome/Safari)
        # to avoid Google OAuth blocking in LINE's in-app browser
        separator = "&" if "?" in url else "?"
        external_url = f"{url}{separator}openExternalBrowser=1"
        lines.extend(["", "詳細はこちら:", shorten_url(external_url)])
    base_message = add_test_prefix("\n".join(lines))
    # Use @all mention to notify all members in the group
    substitution = {
        "mention_all": {
            "type": "mention",
            "mentionee": {"type": "all"},
        }
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
    """Context manager for IMAP connection with timeout protection."""
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)
    mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
    try:
        mail.login(GMAIL_IMAP_USER, GMAIL_IMAP_PASSWORD)
        mail.select("INBOX", readonly=True)
        yield mail
    finally:
        socket.setdefaulttimeout(old_timeout)
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
                        log("Indeed legacyId not found via any tracking URL")
                else:
                    log(f"Indeed legacyId not found (no valid engage URL)")

        if legacy_id:
            # legacyIdが取得できた場合、管理画面URLを生成（engage URLより安定・短縮URL用）
            url = f"https://employers.indeed.com/candidates/view?id={legacy_id}"
            details = fetch_all_details(legacy_id)
            if details:
                phone = details.get("phone") or phone  # APIの方が正確
                indeed_location = details.get("location")
                indeed_email = details.get("email")
                indeed_answers = details.get("answers") or []
                log(f"Indeed API details: phone={phone}, location={indeed_location}, answers={len(indeed_answers or [])}件")
            else:
                log(f"Indeed API returned no details for legacyId={legacy_id} (CTK expired? API error?)")

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

    if phone:
        phone = normalize_phone_number(phone)
    log(f"Notify {source}: {applicant_name}, phone={phone}, url={url}, id={unique_id}")

    slack_ok = notify_slack_with_retry(source, applicant_name, url, phone=phone, location=indeed_location, email_addr=indeed_email, answers=indeed_answers)
    line_ok = notify_line_with_retry(source, applicant_name, url, phone=phone, location=indeed_location, email_addr=indeed_email, answers=indeed_answers)

    if not slack_ok:
        log(f"WARNING: Slack notification failed for {applicant_name} ({unique_id})")
    if not line_ok:
        log(f"WARNING: LINE notification failed for {applicant_name} ({unique_id})")
    if not slack_ok and not line_ok:
        log(f"ERROR: All notifications failed for {applicant_name} ({unique_id}), will retry next cycle")
        return None

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
            if uids_to_mark:
                log(f"Bootstrapping {len(uids_to_mark)} UIDs for already-processed emails")
                for uid_str, gm_id in uids_to_mark:
                    processed_ids.add(f"uid:{uid_str}")
                if not save_processed_ids(processed_ids):
                    log("ERROR: Failed to save bootstrapped UIDs")
                    return True  # Not a quota error

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


# --- Flask Webhook Server (Cowork LINE�() ---
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


@flask_app.route("/notify-line", methods=["POST", "OPTIONS"])
def notify_line_webhook():
    if flask_request.method == "OPTIONS":
        return "", 204
    if not COWORK_WEBHOOK_TOKEN:
        log("ERROR: COWORK_WEBHOOK_TOKEN not configured - rejecting all requests for security")
        return jsonify({"error": "Service not configured"}), 503
    if flask_request.headers.get("X-Cowork-Token", "") != COWORK_WEBHOOK_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    data = flask_request.get_json(force=True) or {}
    name    = data.get("name", "")
    phone   = data.get("phone") or None
    email_addr = data.get("email") or None
    address = data.get("address") or None
    log(f"[webhook] notify-line: name={name}, phone={phone}, email={email_addr}")
    ok = notify_line_with_retry("indeed", name, "", phone=phone, email_addr=email_addr, location=address)
    return jsonify({"ok": ok})


def run_flask_server() -> None:
    port = int(os.getenv("PORT", "8080"))
    log(f"Starting Flask webhook server on port {port}")
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)


# --- Main loop ---
def main() -> None:
    """Main polling loop with exponential backoff for quota errors."""
    # Start Flask webhook server in background thread
    flask_thread = Thread(target=run_flask_server, daemon=True)
    flask_thread.start()

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

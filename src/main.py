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
LINE_TO_ID_PERSONAL = os.getenv("LINE_TO_ID_PERSONAL")  # CTKéç¥ç¨ï¼åäººLINEï¼
COWORK_WEBHOOK_TOKEN = os.getenv("COWORK_WEBHOOK_TOKEN", "")

# CTKæ´æ°ãã©ã¼ã ã®ãã¼ã¹URLï¼Railway ã®ãµã¼ãã¹URLï¼
RAILWAY_SERVICE_URL = os.getenv("RAILWAY_SERVICE_URL", "https://recruit-production-f2dc.up.railway.app")

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
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "20"))  # ããã©ã«ã20ç§
MAX_BACKOFF_SECONDS = int(os.getenv("MAX_BACKOFF_SECONDS", "900"))  # æå¤§15åã®ããã¯ãªã

# --- Search window for emails (days) ---
SEARCH_DAYS = int(os.getenv("SEARCH_DAYS", "1"))  # ããã©ã«ã1æ¥éï¼Gmail APIå¶éå¯¾ç­ï¼

# --- Batch limit per cycle (QUOTA ERRORå¯¾ç­) ---
MAX_EMAILS_PER_CYCLE = int(os.getenv("MAX_EMAILS_PER_CYCLE", "10"))  # 1ãµã¤ã¯ã«ã§å¦çããæå¤§ã¡ã¼ã«æ°

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

def notify_ctk_expired() -> None:
    """Indeed CTK æéåãã LINE ã¨ Slack ã§éç¥ããï¼1ãµã¼ãã¹èµ·åä¸­ã«1åº¦ã ãï¼ã"""
    global _ctk_expired_notified
    with _ctk_expired_notified_lock:
        if _ctk_expired_notified:
            return  # ãã§ã«éç¥æ¸ã¿
        _ctk_expired_notified = True
    log("ALERT: Indeed CTK ãæéåãã§ããLINE/Slack ã«éç¥ãã¾ãã")
    setup_url = f"{RAILWAY_SERVICE_URL}/update-ctk-setup?token={COWORK_WEBHOOK_TOKEN}"
    message = (
        "â ï¸ Indeed CTK ãæéåãã§ã\n\n"
        "é»è©±çªå·ã»ä½æã®åå¾ãã§ãã¾ããã\n"
        "â» å¿åéç¥èªä½ã¯å±ãç¶ãã¾ãã\n\n"
        "ãæ´æ°æé ã\n"
        "â  Chrome ã§ jp.indeed.com ãéã\n"
        "â¡ ãæ°ã«å¥ã âãCTKæ´æ°ããã¿ãã\n"
        "â¢ CTKå¤ãèªåå¥åããããã¼ã¸ãéã\n"
        "â£ãæ´æ°ããããã¿ã³ãæ¼ãã¦å®äº\n\n"
        "â» ã¾ã è¨­å®ãã¦ããªãå ´åã¯ð\n"
        f"{setup_url}"
    )
    # Slackéç¥
    notify_error_to_slack(message)
    # LINEéç¥ï¼åäººLINEã«éä¿¡ãæªè¨­å®ã®å ´åã¯ã°ã«ã¼ãã«ãã©ã¼ã«ããã¯ï¼
    line_to_id = LINE_TO_ID_PERSONAL or get_line_to_id()
    if LINE_CHANNEL_ACCESS_TOKEN and line_to_id:
        body = {
            "to": line_to_id,
            "messages": [{"type": "text", "text": message}],
        }
        headers = {
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                "https://api.line.me/v2/bot/message/push",
                json=body,
                headers=headers,
                timeout=10,
            )
            log(f"CTKæéåã LINEéç¥: status={resp.status_code}")
        except Exception as e:
            log(f"ERROR: CTKæéåã LINEéç¥ å¤±æ: {e}")


def notify_error_to_slack(message: str) -> None:
    """éå¤§ãªã¨ã©ã¼ã Slack Webhook ã«éç¥ãã"""
    webhook_url = SLACK_ERROR_WEBHOOK_URL or SLACK_WEBHOOK_URL_PROD
    if not webhook_url:
        log("ERROR: No Slack webhook URL configured; cannot notify error to Slack")
        return
    text = f"ð¨ Indeedå¿åéç¥ã¨ã©ã¼çºç\n{message}"
    try:
        resp = requests.post(
            webhook_url,
            json={"text": text},
            timeout=5,
        )
        if resp.status_code >= 400:
            log(f"ERROR: failed to send error notification to Slack (status={resp.status_code}, body={resp.text})")
    except Exception as e:
        # éç¥æã®ã¨ã©ã¼ã§ããã«ä¾å¤ãæããã¨ã«ã¼ãããã®ã§ã­ã°ã®ã¿
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
    return f"ããã¹ããã¼ã¸ã§ã³ã\n{message}" if is_test_mode() else message

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
        if "å¿ååå®¹ãç¢ºèªãã" in (a.get_text() or ""):
            return a.get("href") or ""
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if "indeed" in href:
            return href
    return ""

def extract_indeed_legacy_id(html: str) -> Optional[str]:
    """Indeedéç¥ã¡ã¼ã«ã®HTMLããlegacyIdï¼hexï¼ãæ½åºããã
    Indeedéç¥ã¡ã¼ã«ã«ã¯ä»¥ä¸ã®URLãã¿ã¼ã³ãå«ã¾ãã:
    - https://employers.indeed.com/candidates/view?id=<legacyId>
    - https://engage.indeed.com/f/a/<legacyId>~~/... (æ§å½¢å¼: hex)
    - https://engage.indeed.com/f/a/<base64url>~~... (æ°å½¢å¼: base64url 22æå­)
    legacyId ã¯ hexæå­åï¼8ã20æ¡ï¼ã
    """
    if not html:
        return None
    # ãã¿ã¼ã³1: employers.indeed.com ã«ç´æ¥ id= ãã©ã¡ã¼ã¿ãå«ã¾ããå ´å
    direct = re.search(r'employers\.indeed\.com/candidates(?:/view)?\?(?:[^"\'<>\s]*&)?id=([a-f0-9]{8,20})', html)
    if direct:
        return direct.group(1)
    # ãã¿ã¼ã³2: engage.indeed.com/f/a/<hex>~~ å½¢å¼ï¼æ§å½¢å¼ï¼
    engage_hex = re.search(r'engage\.indeed\.com/f/a/([a-f0-9]{10,16})(?:~~|/)', html)
    if engage_hex:
        return engage_hex.group(1)
    # ãã¿ã¼ã³3: ä»»æã®URLã® id= ãã©ã¡ã¼ã¿ï¼indeed ãã¡ã¤ã³åï¼
    any_id = re.search(r'indeed\.com[^"\'<>\s]*[?&]id=([a-f0-9]{8,20})', html)
    if any_id:
        return any_id.group(1)
    return None

def extract_indeed_engage_urls(html: str) -> list:
    """Indeedéç¥ã¡ã¼ã«ã®HTMLããengage.indeed.comãã©ãã­ã³ã°URLãå¨ã¦æ½åºããã
    æ°å½¢å¼(base64url)ã»æ§å½¢å¼(hex)åãã engage.indeed.com/f/a/ URLãè¿ãã
    ãããURLã¯ãªãã¤ã¬ã¯ãããã©ãã¨ employers.indeed.com/candidates/view?id=<hex> ã«å°éããã
    """
    if not html:
        return []
    # engage.indeed.com/f/a/<ä»»æã®æå­å>~~ ãã¿ã¼ã³
    matches = re.findall(r'(https://engage\.indeed\.com/f/a/[A-Za-z0-9_\-]{10,}~~[^\s"\'<>]*)', html)
    return list(dict.fromkeys(matches))  # éè¤é¤å»ï¼é åºä¿æï¼

def extract_phone_number(html: str) -> Optional[str]:
    """ã¡ã¼ã«æ¬æHTMLããé»è©±çªå·ãæ½åºããã"""
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    # æ¥æ¬ã®é»è©±çªå·ãã¿ã¼ã³ï¼æºå¸¯ã»åºå®ã»ããªã¼ãã¤ã¤ã«ï¼
    patterns = [
        r'0[789]0[-\s]?\d{4}[-\s]?\d{4}',  # æºå¸¯: 090/080/070
        r'0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{4}',  # åºå®: 03-xxxx-xxxx ç­
        r'0120[-\s]?\d{3}[-\s]?\d{3}',  # ããªã¼ãã¤ã¤ã«
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    return None

def normalize_phone_number(phone: str) -> str:
    """+81å½¢å¼ãæ¥æ¬å½åå½¢å¼(0XX-XXXX-XXXX)ã«å¤æããã"""
    if not phone:
        return phone
    digits = re.sub(r'[\s\-\(\)]', '', phone)
    if digits.startswith('+81'):
        digits = '0' + digits[3:]
    if re.match(r'^0[789]0\d{8}$', digits):  # æºå¸¯ 090/080/070
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if re.match(r'^0\d{9}$', digits):  # åºå®10æ¡
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if re.match(r'^0120\d{6}$', digits):  # ããªã¼ãã¤ã¤ã«
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return phone

def extract_body_text(html: str, max_chars: int = 500) -> str:
    """ã¡ã¼ã«æ¬æHTMLãããã¬ã¼ã³ãã­ã¹ããæ½åºããï¼æå¤§max_charsæå­ï¼ã"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # script/styleã¿ã°ãé¤å»
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # é£ç¶ããç©ºè¡ã1è¡ã«ã¾ã¨ãã
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    result = "\n".join(lines)
    if len(result) > max_chars:
        result = result[:max_chars] + "â¦"
    return result

def format_phone_for_slack(phone: str) -> str:
    """Format phone number as a Slack tel: link.
    Converts '+81 80 2478 7813' â '<tel:+818024787813|080-2478-7813>'
    so it becomes a tappable link in Slack mobile.
    """
    if not phone:
        return phone
    # Remove spaces to build the tel URI
    tel_uri = phone.replace(" ", "")
    # Build Japanese local display format: +81 80 XXXX XXXX â 080-XXXX-XXXX
    digits = tel_uri.lstrip("+")
    if digits.startswith("81") and len(digits) >= 11:
        local = "0" + digits[2:]  # 81 â 0
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
    Converts '+81 80 2478 7813' â '080-2478-7813'
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
    """Indeedã¡ã¼ã«ã®HTMLæ¬æããå¿åèåãæ½åºããã
    Indeedã®ã¡ã¼ã«ã¯from_headerããIndeed <noreply@indeed.com>ãã®ãã
    ãããã¼ããã¯å¿åèåãåå¾ã§ããªããä»£ããã«ã¡ã¼ã«æ¬æHTMLããåå¾ããã
    è©¦ã¿ããã¿ã¼ã³:
    1. ãââããããã®å¿åããââ ãããå¿åãã¾ãããç­ã®ãã­ã¹ã
    2. ä»¶åãæ°ããå¿åèã®ãç¥ãã: ââãã®ãã¿ã¼ã³
    3. td/div/påã«ãå¿åè:ããå¿åèå:ãç­ã®ã©ãã«ã«ç¶ãåå
    """
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    # ãã¿ã¼ã³1: ãââããããã®å¿åããââãããå¿åãã¾ããã
    for pattern in [
        r"([^\s \n]+(?:\s[^\s \n]+)?)\s*ãã(?:ãã(?:ã®)?å¿å|ãå¿å)",
        r"æ°ããå¿åè(?:ã®ãç¥ãã)?[:ï¼]\s*([^\n\r]+)",
        r"å¿åè(?:å)?[:ï¼]\s*([^\n\r]+)",
        r"([^\s \n]{1,20})\s*(?:æ§|ãã)(?:\s|$|ã|ãã|ã®)",
    ]:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # æããã«ååã§ã¯ãªããã®ãé¤å¤ï¼URLãé·ãããæå­åï¼
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
    mention_prefix = "<!channel>\n"
    if source == "indeed":
        lines = ["ãIndeed æ°çå¿åã"]
        lines.append(f"æ°åï¼{name}")
        if job_title:
            lines.append(f"æ±äººï¼{job_title}")
        lines.append(f"é»è©±ï¼{format_phone_for_slack(phone) if phone else 'æªç»é²'}")
        lines.append(f"ä½æï¼{location if location else 'æªç»é²'}")
        if email_addr:
            lines.append(f"ã¡ã¼ã«ï¼{email_addr}")
        if url:
            lines.append(f"URLï¼{shorten_url(url)}")
    else:
        lines = [f"ãã¸ã¢ãã£ã¼ã ã{name}ã ããããå¿åãããã¾ããã"]
        if job_title:
            lines.append(f"æ±äºº: {job_title}")
        if phone:
            lines.append(f"é»è©±çªå·: {format_phone_for_slack(phone)}")
        if location:
            lines.append(f"ä½æ: {location}")
        if email_addr:
            lines.append(f"ã¡ã¼ã«: {email_addr}")
        if answers:
            for ans in answers:
                key = ans.get("questionKey", "")
                val = ans.get("value")
                if val and key:
                    lines.append(f"{key}: {val}")
        if url:
            lines.extend(["", "å¿ååå®¹ã¯ãã¡ã:", shorten_url(url)])
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
        lines = ["ãIndeed æ°çå¿åã"]
        lines.append(f"æ°åï¼{name}")
        if job_title:
            lines.append(f"æ±äººï¼{job_title}")
        lines.append(f"é»è©±ï¼{format_phone_for_line(phone) if phone else 'æªç»é²'}")
        lines.append(f"ä½æï¼{location if location else 'æªç»é²'}")
        if email_addr:
            lines.append(f"ã¡ã¼ã«ï¼{email_addr}")
        if url:
            lines.append(f"URLï¼{shorten_url(url)}")
    else:
        lines = [f"ã{name}ã ããããã¸ã¢ãã£ã¼ã§æ°çãããã¾ãã"]
        if job_title:
            lines.append(f"æ±äºº: {job_title}")
        if phone:
            lines.append(f"ð é»è©±çªå·: {format_phone_for_line(phone)}")
        if location:
            lines.append(f"ð ä½æ: {location}")
        if email_addr:
            lines.append(f"ð§ ã¡ã¼ã«: {email_addr}")
        if answers:
            for ans in answers:
                key = ans.get("questionKey", "")
                val = ans.get("value")
                if val and key:
                    lines.append(f"ð {key}: {val}")
        if url:
            lines.extend(["", "è©³ç´°ã¯ãã¡ã:", shorten_url(url)])
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
    if "æ°ããå¿åèã®ãç¥ãã" in subject:
        return "indeed", None
    elif "ã¸ã¢ãã£ã¼" in subject:
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
    # Indeedã¡ã¼ã«ã¯From=ãIndeed <noreply@indeed.com>ããªã®ã§
    # ã¡ã¼ã«æ¬æHTMLããå¿åèåãåå¾ãããåããªããã°Fromãããã¼ã®ååãä½¿ãã
    if source == "indeed":
        applicant_name = extract_applicant_name_from_html(html)
        if not applicant_name:
            applicant_name = extract_name(from_header)
    else:
        applicant_name = extract_name(from_header)
    # é»è©±çªå·ã»æ¬æãã­ã¹ããæ½åº
    phone = extract_phone_number(html)
    # Indeedå¿åã®å ´å: URLããlegacyIdãæ½åºãã¦APIã§å¨è©³ç´°ãåå¾
    indeed_location: Optional[str] = None
    indeed_email: Optional[str] = None
    indeed_answers: Optional[list] = None
    if source == "indeed":
        from indeed_fetcher import fetch_all_details, resolve_legacy_id_from_tracking_url, fetch_by_name
        legacy_id = extract_indeed_legacy_id(html)
        if legacy_id:
            log(f"Indeed legacyId found in HTML: {legacy_id}")
        else:
            # HTMLããç´æ¥hex IDãåããªãã£ãå ´å:
            # engage.indeed.com ãã©ãã­ã³ã°URLããã©ã£ã¦hex IDãåå¾ãã
            log("Indeed legacyId not found in HTML, trying engage tracking URL redirect...")
            engage_urls = extract_indeed_engage_urls(html)
            log(f"Indeed engage URLs found: {len(engage_urls)}")
            for engage_url in engage_urls:
                legacy_id = resolve_legacy_id_from_tracking_url(engage_url)
                if legacy_id:
                    log(f"Indeed legacyId resolved via engage URL redirect: {legacy_id} (from {engage_url[:60]}...)")
                    break
            if not legacy_id:
                # ãã©ã¼ã«ããã¯: extract_indeed_url ã§åå¾ããæ±ç¨URLãè©¦ã¿ã
                if url and "engage.indeed.com" in url:
                    legacy_id = resolve_legacy_id_from_tracking_url(url)
                    if legacy_id:
                        log(f"Indeed legacyId resolved via fallback URL redirect: {legacy_id}")
                else:
                    log(f"Indeed legacyId not found (no valid engage URL)")
        if legacy_id:
            # legacyIdãåå¾ã§ããå ´åãç®¡çç»é¢URLãçæï¼engage URLããå®å®ã»ç­ç¸®URLç¨ï¼
            url = f"https://employers.indeed.com/candidates/view?id={legacy_id}"
            details = fetch_all_details(legacy_id)
            if not details:
                # ä¸æçãªAPIéå®³å¯¾ç­: 3ç§å¾ã«å³ãªãã©ã¤ï¼æ¬¡ãµã¤ã¯ã«å¾ã¡ãªãï¼
                log(f"fetch_all_details empty, retrying in 3s...")
                time.sleep(3)
                details = fetch_all_details(legacy_id)
            if details:
                phone = details.get("phone") or phone  # APIã®æ¹ãæ­£ç¢º
                indeed_location = details.get("location")
                indeed_email = details.get("email")
                indeed_answers = details.get("answers") or []
                log(f"Indeed API details: phone={phone}, location={indeed_location}, answers={len(indeed_answers or [])}ä»¶")
            else:
                log(f"Indeed API returned no details for legacyId={legacy_id} after retry (CTK expired?)")
                # CTKæéåãæ¤ç¥ â LINE/Slack ã§éç¥ï¼1åã®ã¿ï¼
                try:
                    from indeed_fetcher import is_ctk_expired
                    if is_ctk_expired():
                        notify_ctk_expired()
                except ImportError:
                    pass
            # ãã©ã¼ã«ããã¯: phoneããªãå ´åãååæ¤ç´¢ã§è£å®ï¼GraphQL APIãé»è©±çªå·ãè¿ããªããã¨ãããï¼
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
            if uids_to_mark:
                log(f"Bootstrapping {len(uids_to_mark)} UIDs for already-processed emails")
                for uid_str, gm_id in uids_to_mark:
                    processed_ids.add(f"uid:{uid_str}")
                if not save_processed_ids(processed_ids):
                    log("ERROR: Failed to save bootstrapped UIDs")
                    return True  # Not a quota error

            if truly_new_uids:
                total_new = len(truly_new_uids)

                # === STARTUP PROTECTION: Detect restart with lost state ===
                # èµ·åç´å¾ã®å®å¨ãã§ãã¯: processed_ids ãç©º or å°ãªãå ´åã
                # æ¢å­ã¡ã¼ã«ã "seen" ã¨ãã¦ãµã¤ã¬ã³ããã¼ã¯ããï¼éç¥ããªãï¼
                # ããã«ããåèµ·åæã®äºééç¥ãé²ã
                with _first_cycle_lock:
                    if not _first_cycle_done and len(processed_ids) == 0 and len(truly_new_uids) > 3:
                        log(f"STARTUP PROTECTION: processed_ids is empty and {len(truly_new_uids)} 'new' emails found.")
                        log(f"This likely means a restart with lost state. Silently marking existing emails as processed...")
                        # å¨ä»¶ãè»½éåå¾ãã¦gm:IDã®ã¿è¨é²ï¼éç¥ãªãï¼
                        for uid in truly_new_uids:
                            uid_str = uid.decode() if isinstance(uid, bytes) else uid
                            gm_id = get_gm_msgid_lightweight(mail, uid_str)
                            if gm_id:
                                processed_ids.add(gm_id)
                                processed_ids.add(f"uid:{uid_str}")
                        save_processed_ids(processed_ids)
                        log(f"STARTUP PROTECTION: Silently marked {len(truly_new_uids)} emails. Next cycle will process only truly new emails.")
                        _first_cycle_done = True
                        return True
                    _first_cycle_done = True

                # QUOTA ERRORå¯¾ç­: 1ãµã¤ã¯ã«ã§å¦çããã¡ã¼ã«æ°ãå¶éãã
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

# --- Flask Webhook Server (Cowork LINE compatible) ---
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

# --- CTKæ´æ°ãã©ã¼ã ï¼ã¢ãã¤ã«å¯¾å¿ï¼ ---
_CTK_UPDATE_FORM_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
  <title>Indeed CTK æ´æ°</title>
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
  <h1>âï¸ Indeed CTK æ´æ°</h1>
  <p class="sub">æ°ããCTKå¤ãè²¼ãä»ãã¦ãæ´æ°ããããæ¼ãã¦ãã ãããåããã­ã¤ä¸è¦ã§å³åæ ããã¾ãã</p>
  <form method="POST">
    <textarea name="ctk" placeholder="CTKå¤ãããã«è²¼ãä»ã..." autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"></textarea>
    <button type="submit">â æ´æ°ãã</button>
  </form>
  <div class="howto">
    <b>ð CTKã®åå¾æé ï¼PCã®Chromeã§ï¼</b>
    <ol>
      <li>jp.indeed.com ã«ã­ã°ã¤ã³</li>
      <li>F12ï¼ã¾ãã¯Ctrl+Shift+Iï¼â Application ã¿ã â Cookies â jp.indeed.com</li>
      <li>ãCTKãã®å¤ãã³ãã¼</li>
      <li>ãã®ãã¼ã¸ã«è²¼ãä»ãã¦éä¿¡</li>
    </ol>
  </div>
  <div class="howto" style="margin-top:10px;">
    <b>ð± ã¹ããã®å ´å</b>
    <ol>
      <li>PCã®Chromeã§ä¸ã®æé ã§CTKãåå¾</li>
      <li>èªåã«ã¡ã¼ã«ç­ã§CTKå¤ãéã</li>
      <li>ã¹ããã§ãã®ãã¼ã¸ãéããè²¼ãä»ãã¦éä¿¡</li>
    </ol>
    <p style="margin:8px 0 0;color:#888;font-size:12px;">â» ã¹ããã®ãã©ã¦ã¶ã§ã¯Cookieãç´æ¥ç¢ºèªã§ããªããããPCã§ã®åå¾ãå¿è¦ã§ãã</p>
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
  <title>CTKæ´æ°å®äº</title>
  <style>
    body { font-family: -apple-system, sans-serif; padding: 40px 24px;
           text-align: center; max-width: 400px; margin: 0 auto; }
    .icon { font-size: 60px; margin-bottom: 16px; }
    h1 { font-size: 24px; color: #16a34a; margin-bottom: 10px; }
    p { color: #555; font-size: 15px; line-height: 1.6; }
  </style>
</head>
<body>
  <div class="icon">â</div>
  <h1>CTKæ´æ°å®äº</h1>
  <p>Indeed APIã®èªè¨¼ãåéããã¾ããã<br>æ¬¡åã®å¿åéç¥ããé»è©±çªå·ã»ä½æãå±ãã¾ãã</p>
</body>
</html>"""

@flask_app.route("/update-ctk-setup", methods=["GET"])
def update_ctk_setup():
    """ããã¯ãã¼ã¯ã¬ããè¨­å®ãã¼ã¸ãã¯ã³ã¿ããCTKæ´æ°ã®ååã»ããã¢ããç¨ã"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or token != COWORK_WEBHOOK_TOKEN:
        return "Unauthorized", 401
    # ããã¯ãã¼ã¯ã¬ããJSï¼jp.indeed.comã®CTKãèªåèª­ã¿åããã¦POSTéä¿¡ï¼
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
  <title>CTKæ´æ° ã¯ã³ã¿ããè¨­å®</title>
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
  <h1>â¡ CTKæ´æ° ã¯ã³ã¿ããè¨­å®</h1>
  <p class="sub">ä¸åº¦è¨­å®ããã°ãæ¬¡åãããã¿ã³1ã¤ã§CTKãæ´æ°ã§ãã¾ãã</p>

  <div class="step">
    <span class="step-num">1</span><span class="step-title">ããã¯ãã¼ã¯ã¬ãããä¿å­ãã</span>
    <div class="step-body">
      ä¸ã®ãã¿ã³ã<b>é·æ¼ãï¼ã¾ãã¯å³ã¯ãªãã¯ï¼âããªã³ã¯ãããã¯ãã¼ã¯ã«è¿½å ã</b>ã§ä¿å­ãã¦ãã ããã<br>
      ååã¯ <b>ãCTKæ´æ°ã</b> ã«ãã¦ããã¨ä¾¿å©ã§ãã
      <a class="bm-link" href="{bookmarklet_js}">â­ CTKæ´æ°ï¼ããã¯ãã¼ã¯ç¨ãã¿ã³ï¼</a>
    </div>
  </div>

  <div class="step">
    <span class="step-num">2</span><span class="step-title">CTKãåãããâ¦</span>
    <div class="step-body">
      â  Chromeã§ <b>jp.indeed.com</b> ãéãï¼ã­ã°ã¤ã³æ¸ã¿ã§ããã°OKï¼<br>
      â¡ ãã©ã¦ã¶ã® <b>â ãæ°ã«å¥ã â ãCTKæ´æ°ã</b> ãã¿ãã<br>
      â¢ CTKå¤ãèªåå¥åããããã¼ã¸ãéã<br>
      â£ ãæ´æ°ããããã¿ã³ãæ¼ãã¦å®äºï¼<br><br>
      <span style="color:#b45309;font-size:13px;">â  èªååå¾ã§ããªãå ´åã¯æåå¥åãã©ã¼ã ã«è»¢éããã¾ãã<br>
      ãã®å ´åã¯PCã®Chromeã§ <b>F12 â Application â Cookies â CTK</b> ã®å¤ãã³ãã¼ãã¦è²¼ãä»ãã¦ãã ããã</span>
    </div>
  </div>

  <div class="after">
    <div class="after-title">â è¨­å®å®äºå¾ã®æé ã¯ããã ã</div>
    <div class="after-body">
      jp.indeed.com ãéã â ãæ°ã«å¥ããããCTKæ´æ°ããã¿ãã â CTKèªåå¥å â ãæ´æ°ããããæ¼ã<br>
      <span style="font-size:13px;color:#166534;">ï¼èªååå¾ã§ããªãå ´åã¯æåå¥åãã©ã¼ã ã§å¯¾å¿å¯ï¼</span>
    </div>
  </div>

  <p class="note">ãã®ãã¼ã¸ã®URLã¯ä¿ç®¡ãã¦ãã ãããæ¬¡åã®ã»ããã¢ããæã«å¿è¦ã§ãã</p>
</body>
</html>"""
    return html


@flask_app.route("/update-ctk", methods=["GET", "POST"])
def update_ctk_endpoint():
    """CTKæ´æ°ãã©ã¼ã ï¼ã¢ãã¤ã«å¯¾å¿ï¼ãCOWORK_WEBHOOK_TOKENã§èªè¨¼ã"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or token != COWORK_WEBHOOK_TOKEN:
        return "Unauthorized", 401

    if flask_request.method == "GET":
        return _CTK_UPDATE_FORM_HTML

    # POST: CTKãæ´æ°ãã¦ãã©ã°ããªã»ãã
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
    # CTKæéåãéç¥ãã©ã°ããªã»ããï¼æ¬¡åæéåãæã«åéç¥ã§ããããã«ï¼
    global _ctk_expired_notified
    with _ctk_expired_notified_lock:
        _ctk_expired_notified = False
    log("CTK flags reset. System will resume normal operation on next poll.")
    return _CTK_UPDATE_SUCCESS_HTML

@flask_app.route("/send-setup-msg", methods=["GET"])
def send_setup_msg():
    """LINEã°ã«ã¼ãã«CTKã»ããã¢ããURLãéä¿¡ããï¼ååè¨­å®ç¨ã»GETå¼ã³åºãå¯ï¼ã"""
    token = flask_request.args.get("token", "")
    if not COWORK_WEBHOOK_TOKEN or token != COWORK_WEBHOOK_TOKEN:
        return "Unauthorized", 401
    setup_url = f"{RAILWAY_SERVICE_URL}/update-ctk-setup?token={COWORK_WEBHOOK_TOKEN}"
    msg = (
        "ãCTKæ´æ° ååè¨­å®ã®ãé¡ãã\n\n"
        "Indeedã®CTKãæéåãã«ãªã£ãã¨ãã\n"
        "ã¯ã³ã¿ããã§æ´æ°ã§ããããã¯ãã¼ã¯ã¬ãããè¨­å®ãã¦ãã ããã\n\n"
        "â  ä¸ã®URLãChromeã§éã\n"
        "â¡ è¡¨ç¤ºãããæé ã«å¾ã£ã¦ããã¯ãã¼ã¯ãè¿½å \n"
        "â¢ æ¬¡åCTKåãéç¥ãæ¥ããããã¯ãã¼ã¯ãã¿ããããã ã\n\n"
        f"{setup_url}"
    )
    line_to_id = LINE_TO_ID_PROD if MODE == "prod" else LINE_TO_ID_TEST
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
            return "â LINEã«éä¿¡ãã¾ãã", 200
        return f"LINE API error: {resp.status_code} {resp.text}", 500
    except Exception as e:
        log(f"[send-setup-msg] error: {e}")
        return f"Error: {e}", 500


@flask_app.route("/send-test-personal", methods=["GET"])
    def send_test_personal():
        """個人LINEにテストメッセージを送信する（GETで呼び出し可）"""
        token = flask_request.args.get("token", "")
        if not COWORK_WEBHOOK_TOKEN or token != COWORK_WEBHOOK_TOKEN:
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
    if flask_request.headers.get("X-Cowork-Token", "") != COWORK_WEBHOOK_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    data = flask_request.get_json(force=True) or {}
    name = data.get("name", "")
    phone = data.get("phone") or None
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

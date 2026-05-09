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
from bs4 import BeautifulSoup
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
LOG_DIR = os.getenv("LOG_DIR", "/tmp")
SLACK_ERROR_WEBHOOK_URL = os.getenv("SLACK_ERROR_WEBHOOK_URL")

# --- Processed IDs file for duplicate prevention ---
PROCESSED_IDS_FILE = os.getenv("PROCESSED_IDS_FILE", os.path.join(LOG_DIR, "processed_ids.json"))


# --- Polling Interval ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))  # ããã©ã«ã60ç§ï¼Gmail APIå¶éå¯¾ç­ï¼
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
        r"([^\sã\n]+(?:\s[^\sã\n]+)?)\s*ãã(?:ãã(?:ã®)?å¿å|ãå¿å)",
        r"æ°ããå¿åè(?:ã®ãç¥ãã)?[:ï¼]\s*([^\n\r]+)",
        r"å¿åè(?:å)?[:ï¼]\s*([^\n\r]+)",
        r"([^\sã\n]{1,20})\s*(?:æ§|ãã)(?:\s|$|ã|ãã|ã®)",
    ]:
        match = re.search(pattern, text)
        if match:
            name = match.group(1).strip()
            # æããã«ååã§ã¯ãªããã®ãé¤å¤ï¼URLãé·ãããæå­åï¼
            if name and len(name) <= 30 and "http" not in name and "@" not in name:
                return name

    return None


def shorten_url(url: str) -> str:
    """Shorten URL using is.gd (primary) and TinyURL (fallback).

    is.gd is preferred because TinyURL has been observed to time out
    from Railway (see commit a5b0e85 history). Returns the original URL
    if both shorteners fail.
    """
    if not url:
        return url
    # Primary: is.gd
    try:
        api = "https://is.gd/create.php?format=simple&url=" + requests.utils.quote(url, safe="")
        resp = requests.get(api, timeout=5)
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
        log(f"WARN: is.gd shorten returned status={resp.status_code} body={resp.text[:80]}")
    except Exception as e:
        log(f"WARN: is.gd shorten failed: {e}")
    # Fallback: TinyURL
    try:
        api = "https://tinyurl.com/api-create.php?url=" + requests.utils.quote(url, safe="")
        resp = requests.get(api, timeout=5)
        if resp.status_code == 200 and resp.text.strip().startswith("http"):
            return resp.text.strip()
        log(f"WARN: TinyURL shorten returned status={resp.status_code}")
    except Exception as e:
        log(f"WARN: TinyURL shorten failed: {e}")
    log("WARN: Both URL shorteners failed, using original URL")
    return url


# --- Notification Functions ---
def notify_slack_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, max_retries: int = 3) -> bool:
    """Send notification to Slack with retry logic. Returns True if successful."""
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        log("No Slack Webhook URL")
        return False
    title = "ãIndeedå¿åã" if source == "indeed" else "ãã¸ã¢ãã£ã¼ã"
    mention_prefix = "<!channel>\n"

    lines = [f"{title} ã{name}ã ããããå¿åãããã¾ããã"]
    if job_title:
        lines.append(f"æ±äºº: {job_title}")
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


def notify_line_with_retry(source: str, name: str, url: str, job_title: Optional[str] = None, max_retries: int = 3) -> bool:
    """Send notification to LINE with retry logic. Returns True if successful."""
    line_to_id = get_line_to_id()
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return False
    title = "Indeedã«å¿åãããã¾ããã" if source == "indeed" else "ã¸ã¢ãã£ã¼ã§æ°çãããã¾ãã"
    lines = [f"ã{name}ã ãããã{title}"]
    if job_title:
        lines.append(f"æ±äºº: {job_title}")
    if url:
        # Force LINE to open URL in external browser (Chrome/Safari)
        # to avoid Google OAuth blocking in LINE's in-app browser
        separator = "&" if "?" in url else "?"
        external_url = f"{url}{separator}openExternalBrowser=1"
        lines.extend(["", "è©³ç´°ã¯ãã¡ã:", shorten_url(external_url)])
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

    log(f"Notify {source}: {applicant_name}, url={url}, id={unique_id}")

    slack_ok = notify_slack_with_retry(source, applicant_name, url)
    line_ok = notify_line_with_retry(source, applicant_name, url)

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


# --- Main loop ---
def main() -> None:
    """Main polling loop with exponential backoff for quota errors."""
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

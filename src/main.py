"""Gmail polling service for Indeed/Jimoty job application notifications."""

import imaplib
import email
from email.header import decode_header
import os
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

# --- Mention IDs ---
SLACK_MENTION_INOUE_ID = os.getenv("SLACK_MENTION_INOUE_ID")
SLACK_MENTION_KONDO_ID = os.getenv("SLACK_MENTION_KONDO_ID")
LINE_MENTION_INOUE_ID = os.getenv("LINE_MENTION_INOUE_ID")
LINE_MENTION_KONDO_ID = os.getenv("LINE_MENTION_KONDO_ID")

# --- Polling Interval ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))  # デフォルト30秒（Gmail API制限対策）

# --- Search window for emails (days) ---
SEARCH_DAYS = int(os.getenv("SEARCH_DAYS", "7"))  # デフォルト7日間


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
    """Save processed message IDs to file. Returns True if successful."""
    if not ensure_processed_ids_dir():
        return False
    try:
        with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(processed_ids), f)
        log(f"Saved {len(processed_ids)} processed IDs to {PROCESSED_IDS_FILE}")
        return True
    except IOError as e:
        log(f"ERROR: Failed to save processed IDs: {e}")
        notify_error_to_slack(f"Failed to save processed IDs: {e}")
        return False


def notify_error_to_slack(message: str) -> None:
    """重大なエラーを Slack Webhook に通知する"""
    if not SLACK_ERROR_WEBHOOK_URL:
        # Webhook が未設定ならログだけ残す
        log("ERROR: SLACK_ERROR_WEBHOOK_URL is not set; cannot notify error to Slack")
        return

    text = f"🚨 Indeed応募通知エラー発生\n{message}"

    try:
        resp = requests.post(
            SLACK_ERROR_WEBHOOK_URL,
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


def extract_name(from_header: str) -> str:
    """Extract applicant name from From header."""
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


# --- Notification Functions ---
def notify_slack(source: str, name: str, url: str) -> None:
    """Send notification to Slack."""
    webhook_url = get_slack_webhook_url()
    if not webhook_url:
        log("No Slack Webhook URL")
        return

    title = "【Indeed応募】" if source == "indeed" else "【ジモティー】"

    mention_prefix = ""
    if SLACK_MENTION_INOUE_ID and SLACK_MENTION_KONDO_ID:
        mention_prefix = f"<@{SLACK_MENTION_INOUE_ID}> <@{SLACK_MENTION_KONDO_ID}>\n"
    else:
        log("WARNING: Slack mention IDs not fully configured")

    lines = [f"{title} 【{name}】 さんから応募がありました。"]
    if url:
        lines.extend(["", "応募内容はこちら:", url])

    message = add_test_prefix(mention_prefix + "\n".join(lines))

    try:
        resp = requests.post(webhook_url, json={"text": message})
        if resp.status_code >= 400:
            log(f"ERROR: Slack notify failed (status={resp.status_code}, body={resp.text})")
            notify_error_to_slack(f"Slack notify failed: status={resp.status_code}")
    except Exception as e:
        log(f"ERROR: Slack notify exception: {e}")
        notify_error_to_slack(f"Slack notify exception: {e}")


def notify_line(source: str, name: str, url: str) -> None:
    """Send notification to LINE."""
    line_to_id = get_line_to_id()
    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return

    title = "Indeedに応募がありました。" if source == "indeed" else "ジモティーで新着があります。"

    lines = [f"【{name}】 さんから{title}"]
    if url:
        lines.extend(["", "詳細はこちら:", url])

    base_message = add_test_prefix("\n".join(lines))

    # Build mention placeholders and substitution based on configured IDs
    mention_parts = []
    substitution = {}
    if LINE_MENTION_INOUE_ID:
        mention_parts.append("{inoue}")
        substitution["inoue"] = {
            "type": "mention",
            "mentionee": {"type": "user", "userId": LINE_MENTION_INOUE_ID},
        }
    if LINE_MENTION_KONDO_ID:
        mention_parts.append("{kondo}")
        substitution["kondo"] = {
            "type": "mention",
            "mentionee": {"type": "user", "userId": LINE_MENTION_KONDO_ID},
        }

    # Build text_v2 with only configured mention placeholders
    if mention_parts:
        text_v2 = f"{' '.join(mention_parts)} {base_message}"
    else:
        text_v2 = base_message
        log("WARNING: LINE mention IDs not configured; sending without mentions")

    body = {
        "to": line_to_id,
        "messages": [{"type": "textV2", "text": text_v2, "substitution": substitution}],
    }

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post("https://api.line.me/v2/bot/message/push", json=body, headers=headers)
        log(f"LINE API response: status={resp.status_code}")
        if resp.status_code >= 400:
            log(f"ERROR: LINE notify failed (status={resp.status_code}, body={resp.text})")
            notify_error_to_slack(f"LINE notify failed: status={resp.status_code}, body={resp.text}")
    except Exception as e:
        log(f"ERROR: LINE notify exception: {e}")
        notify_error_to_slack(f"LINE notify exception: {e}")


# --- IMAP Connection ---
@contextmanager
def imap_connection():
    """Context manager for IMAP connection."""
    mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
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


def process_mail(
    mail: imaplib.IMAP4_SSL, msg_id: bytes, processed_ids: Set[str]
) -> Optional[str]:
    """Process a single mail. Returns unique ID if processed, None otherwise."""
    status, data = mail.fetch(msg_id, "(X-GM-MSGID BODY.PEEK[])")
    if status != "OK":
        log(f"ERROR: Failed to fetch msg_id={msg_id}, status={status}")
        return None

    gm_msgid, body_data = parse_fetch_response(data)

    if not body_data:
        log(f"ERROR: Failed to fetch body for msg_id={msg_id}")
        return None

    msg = email.message_from_bytes(body_data)
    
    # Get unique identifier (X-GM-MSGID or Message-ID)
    unique_id = get_unique_id(gm_msgid, msg)
    
    if not unique_id:
        log(f"ERROR: No unique ID found for msg_id={msg_id}, skipping to prevent duplicates")
        return None
    
    if unique_id in processed_ids:
        return None  # Already processed, skip silently
    
    subject = decode_header_value(msg.get("Subject", ""))
    from_header = decode_header_value(msg.get("From", ""))
    name = extract_name(from_header)

    source, default_url = determine_source(subject)

    if not source:
        log(f"Skip non-target mail: {subject[:50]}...")
        return unique_id  # Mark as processed to avoid re-checking

    url = extract_indeed_url(extract_html(msg)) if source == "indeed" else default_url
    log(f"Notify {source}: {name}, url={url}, id={unique_id}")

    notify_slack(source, name, url)
    notify_line(source, name, url)

    return unique_id


def get_uid_from_fetch(data: list) -> Optional[str]:
    """Extract UID from IMAP fetch response."""
    for item in data:
        if isinstance(item, tuple):
            header = item[0].decode() if isinstance(item[0], bytes) else item[0]
            match = re.search(r"UID (\d+)", header)
            if match:
                return match.group(1)
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
    name = extract_name(from_header)

    source, default_url = determine_source(subject)

    if not source:
        log(f"Skip non-target mail: {subject[:50]}...")
        return unique_id  # Mark as processed to avoid re-checking

    url = extract_indeed_url(extract_html(msg)) if source == "indeed" else default_url
    log(f"Notify {source}: {name}, url={url}, id={unique_id}")

    notify_slack(source, name, url)
    notify_line(source, name, url)

    return unique_id


def get_gm_msgid_lightweight(mail: imaplib.IMAP4_SSL, uid: str) -> Optional[str]:
    """Fetch only X-GM-MSGID for a single email (lightweight, no body)."""
    status, data = mail.uid("fetch", uid, "(X-GM-MSGID)")
    if status != "OK":
        return None
    
    for item in data:
        if isinstance(item, tuple):
            header = item[0].decode() if isinstance(item[0], bytes) else str(item[0])
            match = re.search(r"X-GM-MSGID (\d+)", header)
            if match:
                return f"gm:{match.group(1)}"
    return None


def check_mail()-> None:
    """Check mailbox for new applications."""
    try:
        processed_ids, load_success = load_processed_ids()
        
        # If file exists but is corrupted, skip processing to prevent mass re-notifications
        if not load_success:
            log("ERROR: Skipping mail check due to corrupted processed IDs file")
            return

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
                log(f"Truly new emails to process: {len(truly_new_uids)}")
            
            # Phase 3: Full processing for truly new emails only
            for uid in truly_new_uids:
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

    except Exception as e:
        log(f"ERROR: {e}")
        notify_error_to_slack(f"Gmail polling error: {e}")


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
    """Main polling loop."""
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")
    log(f"MODE={MODE}, SEARCH_DAYS={SEARCH_DAYS}")
    
    # Verify storage is working
    if not verify_storage():
        log("CRITICAL: Storage verification failed. Exiting to prevent duplicate notifications.")
        notify_error_to_slack("CRITICAL: Storage verification failed at startup. Service stopped.")
        return
    
    while True:
        check_mail()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

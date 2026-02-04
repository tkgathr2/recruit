"""Gmail polling service for Indeed/Jimoty job application notifications."""

import imaplib
import email
from email.header import decode_header
import os
import time
import json
import re
from contextlib import contextmanager
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
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))  # デフォルト15秒

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


def load_processed_ids() -> Set[str]:
    """Load processed message IDs from file."""
    if os.path.exists(PROCESSED_IDS_FILE):
        try:
            with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, IOError) as e:
            log(f"WARNING: Failed to load processed IDs: {e}")
    return set()


def save_processed_ids(processed_ids: Set[str]) -> None:
    """Save processed message IDs to file."""
    try:
        with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(processed_ids), f)
    except IOError as e:
        log(f"ERROR: Failed to save processed IDs: {e}")


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
                return part.get_payload(decode=True).decode(charset, errors="replace")
    elif msg.get_content_type() == "text/html":
        charset = msg.get_content_charset() or "utf-8"
        return msg.get_payload(decode=True).decode(charset, errors="replace")
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
    text_v2 = f"{{inoue}} {{kondo}} {base_message}"

    substitution = {}
    if LINE_MENTION_INOUE_ID:
        substitution["inoue"] = {
            "type": "mention",
            "mentionee": {"type": "user", "userId": LINE_MENTION_INOUE_ID},
        }
    if LINE_MENTION_KONDO_ID:
        substitution["kondo"] = {
            "type": "mention",
            "mentionee": {"type": "user", "userId": LINE_MENTION_KONDO_ID},
        }

    if not substitution:
        log("WARNING: LINE mention IDs not configured; sending without substitution")

    body = {
        "to": line_to_id,
        "messages": [{"type": "textV2", "text": text_v2, "substitution": substitution}],
    }

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = requests.post("https://api.line.me/v2/bot/message/push", json=body, headers=headers)
    log(f"LINE API response: status={resp.status_code}")
    if resp.status_code >= 400:
        notify_error_to_slack(f"LINE notify failed: status={resp.status_code}, body={resp.text}")


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


def process_mail(
    mail: imaplib.IMAP4_SSL, msg_id: bytes, processed_ids: Set[str]
) -> Optional[str]:
    """Process a single mail. Returns X-GM-MSGID if processed, None otherwise."""
    status, data = mail.fetch(msg_id, "(X-GM-MSGID BODY.PEEK[])")
    if status != "OK":
        return None

    gm_msgid, body_data = parse_fetch_response(data)

    if not body_data:
        log(f"ERROR: Failed to fetch body for msg_id={msg_id}")
        return None

    if gm_msgid and gm_msgid in processed_ids:
        log(f"Skip already processed: X-GM-MSGID={gm_msgid}")
        return None

    msg = email.message_from_bytes(body_data)
    subject = decode_header_value(msg.get("Subject", ""))
    from_header = decode_header_value(msg.get("From", ""))
    name = extract_name(from_header)

    source, default_url = determine_source(subject)

    if not source:
        log(f"Skip mail: {subject}")
        return gm_msgid

    url = extract_indeed_url(extract_html(msg)) if source == "indeed" else default_url
    log(f"Notify {source}: {name}, url={url}")

    notify_slack(source, name, url)
    notify_line(source, name, url)

    return gm_msgid


def check_mail() -> None:
    """Check mailbox for new applications."""
    try:
        processed_ids = load_processed_ids()

        with imap_connection() as mail:
            since_date = (datetime.now() - timedelta(days=SEARCH_DAYS)).strftime("%d-%b-%Y")
            status, data = mail.search(None, "SINCE", since_date)

            msg_ids = data[0].split()
            log(f"Emails in last {SEARCH_DAYS} days: {len(msg_ids)}")

            new_processed = False
            for msg_id in msg_ids:
                gm_msgid = process_mail(mail, msg_id, processed_ids)
                if gm_msgid:
                    processed_ids.add(gm_msgid)
                    new_processed = True

            if new_processed:
                save_processed_ids(processed_ids)

    except Exception as e:
        log(f"ERROR: {e}")
        notify_error_to_slack(f"Gmail polling error: {e}")


# --- Main loop ---
def main() -> None:
    """Main polling loop."""
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")

    while True:
        check_mail()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

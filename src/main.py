import imaplib
import email
from email.header import decode_header
import os
import time
import json
import re
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
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    with open(os.path.join(LOG_DIR, "recruit.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")
    # 標準出力にも出力（Railway Logs用）
    print(line, flush=True)


def load_processed_ids():
    """Load processed message IDs from file"""
    if os.path.exists(PROCESSED_IDS_FILE):
        try:
            with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, IOError) as e:
            log(f"WARNING: Failed to load processed IDs: {e}")
    return set()


def save_processed_ids(processed_ids):
    """Save processed message IDs to file"""
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
def get_mode():
    """Return current mode: 'test' or 'prod'"""
    mode = MODE.lower() if MODE else "prod"
    return "test" if mode == "test" else "prod"


def get_slack_webhook_url(mode):
    """Get Slack Webhook URL based on mode"""
    if mode == "test":
        url = SLACK_WEBHOOK_URL_TEST
        if not url:
            log("WARNING: SLACK_WEBHOOK_URL_TEST is not set")
        return url
    else:
        url = SLACK_WEBHOOK_URL_PROD
        if not url:
            log("WARNING: SLACK_WEBHOOK_URL_PROD is not set")
        return url


def get_line_to_id(mode):
    """Get LINE TO ID based on mode"""
    if mode == "test":
        to_id = LINE_TO_ID_TEST
        if not to_id:
            log("WARNING: LINE_TO_ID_TEST is not set")
        return to_id
    else:
        to_id = LINE_TO_ID_PROD
        if not to_id:
            log("WARNING: LINE_TO_ID_PROD is not set")
        return to_id


def add_test_prefix(message, mode):
    """Add test version prefix if in test mode"""
    if mode == "test":
        return f"【テストバージョン】\n{message}"
    return message


# --- Decode header ---
def decode(value):
    if not value:
        return ""
    parts = decode_header(value)
    decoded = ""
    for text, enc in parts:
        if isinstance(text, bytes):
            decoded += text.decode(enc or "utf-8", errors="replace")
        else:
            decoded += text
    return decoded


# --- Extract applicant name ---
def extract_name(from_header):
    try:
        return from_header.split("<")[0].replace('"', "").strip()
    except:
        return from_header


# --- Extract HTML part ---
def extract_html(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                charset = part.get_content_charset() or "utf-8"
                return part.get_payload(decode=True).decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/html":
            charset = msg.get_content_charset() or "utf-8"
            return msg.get_payload(decode=True).decode(charset, errors="replace")
    return ""


# --- Extract URL from Indeed email ---
def extract_indeed_url(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    # Find button "応募内容を確認する"
    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip()
        if "応募内容を確認する" in text:
            return a.get("href") or ""

    # fallback (rare)
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if "indeed" in href:
            return href

    return ""


# --- Slack notify ---
def notify_slack(source, name, url):
    mode = get_mode()
    webhook_url = get_slack_webhook_url(mode)

    if not webhook_url:
        log("No Slack Webhook URL")
        return

    title = "【Indeed応募】" if source == "indeed" else "【ジモティー】"

    # メンション用プレフィックス
    mention_prefix = ""
    if SLACK_MENTION_INOUE_ID and SLACK_MENTION_KONDO_ID:
        mention_prefix = f"<@{SLACK_MENTION_INOUE_ID}> <@{SLACK_MENTION_KONDO_ID}>\n"
    else:
        log("WARNING: Slack mention IDs not fully configured")

    lines = [f"{title} 【{name}】 さんから応募がありました。"]
    if url:
        lines += ["", "応募内容はこちら:", url]

    message = mention_prefix + "\n".join(lines)
    message = add_test_prefix(message, mode)

    try:
        resp = requests.post(webhook_url, json={"text": message})
        if resp.status_code >= 400:
            log(f"ERROR: Slack notify failed (status={resp.status_code}, body={resp.text})")
            notify_error_to_slack(f"Slack notify failed: status={resp.status_code}")
    except Exception as e:
        log(f"ERROR: Slack notify exception: {e}")
        notify_error_to_slack(f"Slack notify exception: {e}")


# --- LINE notify ---
def notify_line(source, name, url):
    mode = get_mode()
    line_to_id = get_line_to_id(mode)

    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return

    title = "Indeedに応募がありました。" if source == "indeed" else "ジモティーで新着があります。"

    # 本文部分
    lines = [f"【{name}】 さんから{title}"]
    if url:
        lines += ["", "詳細はこちら:", url]

    base_message = "\n".join(lines)

    # MODE に応じてテストプレフィックスを適用
    base_message = add_test_prefix(base_message, mode)

    # textV2 + substitution 方式でメンションを実装
    text_v2 = f"{{inoue}} {{kondo}} {base_message}"

    substitution = {}
    if LINE_MENTION_INOUE_ID:
        substitution["inoue"] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": LINE_MENTION_INOUE_ID,
            },
        }
    if LINE_MENTION_KONDO_ID:
        substitution["kondo"] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": LINE_MENTION_KONDO_ID,
            },
        }

    if not substitution:
        log("WARNING: LINE mention IDs not configured; sending without substitution")

    message_obj = {
        "type": "textV2",
        "text": text_v2,
        "substitution": substitution,
    }

    body = {
        "to": line_to_id,
        "messages": [message_obj],
    }

    # デバッグ用ログ
    log(f"LINE base_message: {base_message}")
    log(f"LINE textV2: {text_v2}")
    log(f"LINE substitution: {substitution}")

    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    resp = requests.post("https://api.line.me/v2/bot/message/push", json=body, headers=headers)
    log(f"LINE API response: status={resp.status_code}, body={resp.text}")
    if resp.status_code >= 400:
        notify_error_to_slack(f"LINE notify failed: status={resp.status_code}, body={resp.text}")


# --- Process mail ---
def process_mail(mail, msg_id, processed_ids):
    """Process a single mail. Returns X-GM-MSGID if processed, None otherwise."""
    # Fetch with BODY.PEEK[] to avoid marking as read, and get X-GM-MSGID for duplicate prevention
    status, data = mail.fetch(msg_id, "(X-GM-MSGID BODY.PEEK[])")
    if status != "OK":
        return None

    # Parse response to extract X-GM-MSGID and body
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

    if not body_data:
        log(f"ERROR: Failed to fetch body for msg_id={msg_id}")
        return None

    # Check if already processed
    if gm_msgid and gm_msgid in processed_ids:
        log(f"Skip already processed: X-GM-MSGID={gm_msgid}")
        return None

    msg = email.message_from_bytes(body_data)

    subject = decode(msg.get("Subject", ""))
    from_header = decode(msg.get("From", ""))
    name = extract_name(from_header)
    html = extract_html(msg)

    # Determine source
    if "新しい応募者のお知らせ" in subject:
        source = "indeed"
        url = extract_indeed_url(html)
    elif "ジモティー" in subject:
        source = "jimoty"
        url = "https://jmty.jp/web_mail/posts"
    else:
        log(f"Skip mail: {subject}")
        # Do NOT mark as read - just return the ID to track as processed
        return gm_msgid

    log(f"Notify {source}: {name}, url={url}")

    notify_slack(source, name, url)
    notify_line(source, name, url)

    # Do NOT mark as read - return the ID to track as processed
    return gm_msgid


# --- Check mail once ---
def check_mail():
    try:
        # Load processed IDs at the start of each check
        processed_ids = load_processed_ids()

        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
        mail.login(GMAIL_IMAP_USER, GMAIL_IMAP_PASSWORD)
        mail.select("INBOX", readonly=True)  # Use readonly to prevent any implicit read marking

        # Search for emails from the last N days (regardless of read status)
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

        # Save processed IDs if any new ones were added
        if new_processed:
            save_processed_ids(processed_ids)

        mail.close()
        mail.logout()

    except Exception as e:
        log(f"ERROR: {e}")
        notify_error_to_slack(f"Gmail polling error: {e}")


# --- Main loop ---
def main():
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")

    while True:
        check_mail()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

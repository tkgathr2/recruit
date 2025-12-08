import imaplib
import email
from email.header import decode_header
import os
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

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

# --- Polling Interval ---
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))  # デフォルト15秒


# --- Logging ---
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(os.path.join(LOG_DIR, "recruit.log"), "a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")


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
    lines = [f"{title} {name} さんから応募がありました。"]
    if url:
        lines += ["", "応募内容はこちら:", url]

    message = "\n".join(lines)
    message = add_test_prefix(message, mode)

    requests.post(webhook_url, json={"text": message})


# --- LINE notify ---
def notify_line(source, name, url):
    mode = get_mode()
    line_to_id = get_line_to_id(mode)

    if not LINE_CHANNEL_ACCESS_TOKEN or not line_to_id:
        log("LINE Token or TO ID missing")
        return

    title = "Indeedに応募がありました。" if source == "indeed" else "ジモティーで新着があります。"
    lines = [f"{name} さんから{title}"]
    if url:
        lines += ["", "詳細はこちら:", url]

    message = "\n".join(lines)
    message = add_test_prefix(message, mode)

    body = {
        "to": line_to_id,
        "messages": [{"type": "text", "text": message}],
    }

    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    requests.post("https://api.line.me/v2/bot/message/push", json=body, headers=headers)


# --- Process mail ---
def process_mail(mail, msg_id):
    status, data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        return

    msg = email.message_from_bytes(data[0][1])

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
        mail.store(msg_id, "+FLAGS", "\\Seen")
        return

    log(f"Notify {source}: {name}, url={url}")

    notify_slack(source, name, url)
    notify_line(source, name, url)

    mail.store(msg_id, "+FLAGS", "\\Seen")


# --- Check mail once ---
def check_mail():
    try:
        mail = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
        mail.login(GMAIL_IMAP_USER, GMAIL_IMAP_PASSWORD)
        mail.select("INBOX")
        status, data = mail.search(None, "UNSEEN")

        msg_ids = data[0].split()
        log(f"Unread count: {len(msg_ids)}")

        for msg_id in msg_ids:
            process_mail(mail, msg_id)

        mail.close()
        mail.logout()

    except Exception as e:
        log(f"ERROR: {e}")


# --- Main loop ---
def main():
    log(f"Starting Gmail polling with POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS}")

    while True:
        check_mail()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

"""Unit tests for src/main.py"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.main import (
    decode_header_value,
    extract_name,
    extract_html,
    extract_indeed_url,
    determine_source,
    is_indeed_non_application_email,
    is_from_indeed,
    is_auth_failure,
    has_imap_credentials,
    check_mail_with_status,
    parse_fetch_response,
    load_processed_ids,
    save_processed_ids,
    ensure_processed_ids_dir,
    get_unique_id,
    verify_storage,
    migrate_old_id_format,
    is_test_mode,
    add_test_prefix,
    notify_slack_with_retry,
    notify_line_with_retry,
    process_mail_by_uid,
    extract_job_title_from_subject,
    extract_job_title_from_html,
)
import imaplib
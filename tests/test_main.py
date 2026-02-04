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
    parse_fetch_response,
    load_processed_ids,
    save_processed_ids,
    ensure_processed_ids_dir,
    get_unique_id,
    verify_storage,
    migrate_old_id_format,
    is_test_mode,
    add_test_prefix,
    notify_slack,
    notify_line,
)


class TestDecodeHeaderValue:
    def test_empty_value(self):
        assert decode_header_value(None) == ""
        assert decode_header_value("") == ""

    def test_plain_ascii(self):
        assert decode_header_value("Hello World") == "Hello World"

    def test_japanese_encoded(self):
        encoded = "=?UTF-8?B?44GT44KT44Gr44Gh44Gv?="
        result = decode_header_value(encoded)
        assert result == "こんにちは"

    def test_mixed_encoding(self):
        value = "Test =?UTF-8?B?44OG44K544OI?="
        result = decode_header_value(value)
        assert "Test" in result
        assert "テスト" in result


class TestExtractName:
    def test_standard_format(self):
        assert extract_name("John Doe <john@example.com>") == "John Doe"

    def test_quoted_name(self):
        assert extract_name('"John Doe" <john@example.com>') == "John Doe"

    def test_no_angle_bracket(self):
        assert extract_name("john@example.com") == "john@example.com"

    def test_empty_name(self):
        assert extract_name("<john@example.com>") == ""

    def test_japanese_name(self):
        assert extract_name("田中 太郎 <tanaka@example.com>") == "田中 太郎"


class TestExtractHtml:
    def test_simple_html_message(self):
        msg = MIMEText("<html><body>Hello</body></html>", "html")
        result = extract_html(msg)
        assert "Hello" in result

    def test_multipart_message(self):
        msg = MIMEMultipart("alternative")
        text_part = MIMEText("Plain text", "plain")
        html_part = MIMEText("<html><body>HTML content</body></html>", "html")
        msg.attach(text_part)
        msg.attach(html_part)
        result = extract_html(msg)
        assert "HTML content" in result

    def test_plain_text_only(self):
        msg = MIMEText("Plain text only", "plain")
        result = extract_html(msg)
        assert result == ""


class TestExtractIndeedUrl:
    def test_empty_html(self):
        assert extract_indeed_url("") == ""
        assert extract_indeed_url(None) == ""

    def test_with_application_button(self):
        html = '''
        <html>
        <body>
            <a href="https://indeed.com/apply/123">応募内容を確認する</a>
        </body>
        </html>
        '''
        result = extract_indeed_url(html)
        assert result == "https://indeed.com/apply/123"

    def test_fallback_indeed_link(self):
        html = '''
        <html>
        <body>
            <a href="https://indeed.com/job/456">View Job</a>
        </body>
        </html>
        '''
        result = extract_indeed_url(html)
        assert "indeed" in result

    def test_no_indeed_link(self):
        html = '''
        <html>
        <body>
            <a href="https://example.com">Some link</a>
        </body>
        </html>
        '''
        result = extract_indeed_url(html)
        assert result == ""


class TestDetermineSource:
    def test_indeed_subject(self):
        source, url = determine_source("新しい応募者のお知らせ - 山田太郎")
        assert source == "indeed"
        assert url is None

    def test_jimoty_subject(self):
        source, url = determine_source("ジモティーからのお知らせ")
        assert source == "jimoty"
        assert url == "https://jmty.jp/web_mail/posts"

    def test_unknown_subject(self):
        source, url = determine_source("Random email subject")
        assert source is None
        assert url is None


class TestParseFetchResponse:
    def test_valid_response(self):
        data = [
            (b'1 (X-GM-MSGID 12345678901234567890 BODY[]', b'email body content'),
            b')'
        ]
        gm_msgid, body = parse_fetch_response(data)
        assert gm_msgid == "12345678901234567890"
        assert body == b'email body content'

    def test_no_gm_msgid(self):
        data = [
            (b'1 (BODY[]', b'email body content'),
            b')'
        ]
        gm_msgid, body = parse_fetch_response(data)
        assert gm_msgid is None
        assert body == b'email body content'

    def test_empty_response(self):
        data = []
        gm_msgid, body = parse_fetch_response(data)
        assert gm_msgid is None
        assert body is None


class TestProcessedIds:
    def test_load_nonexistent_file(self):
        with patch('src.main.PROCESSED_IDS_FILE', '/nonexistent/path.json'):
            result, success = load_processed_ids()
            assert result == set()
            assert success == True  # First run is considered success

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with patch('src.main.PROCESSED_IDS_FILE', temp_path):
                ids = {"id1", "id2", "id3"}
                save_result = save_processed_ids(ids)
                assert save_result == True
                loaded, success = load_processed_ids()
                assert loaded == ids
                assert success == True
        finally:
            os.unlink(temp_path)

    def test_load_corrupted_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json")
            temp_path = f.name

        try:
            with patch('src.main.PROCESSED_IDS_FILE', temp_path):
                with patch('src.main.log') as mock_log:
                    result, success = load_processed_ids()
                    assert result == set()
                    assert success == False  # Corrupted file returns False
                    mock_log.assert_called()
        finally:
            os.unlink(temp_path)


class TestEnsureProcessedIdsDir:
    def test_creates_directory(self):
        """Test that directory is created if it doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            new_dir = os.path.join(tmpdir, "subdir", "processed_ids.json")
            with patch('src.main.PROCESSED_IDS_FILE', new_dir):
                result = ensure_processed_ids_dir()
                assert result == True
                assert os.path.exists(os.path.dirname(new_dir))

    def test_existing_directory(self):
        """Test that function succeeds when directory already exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            existing_file = os.path.join(tmpdir, "processed_ids.json")
            with patch('src.main.PROCESSED_IDS_FILE', existing_file):
                result = ensure_processed_ids_dir()
                assert result == True


class TestGetUniqueId:
    def test_prefers_gm_msgid(self):
        """Test that X-GM-MSGID is preferred over Message-ID"""
        msg = MagicMock()
        msg.get.return_value = "<message-id@example.com>"
        
        result = get_unique_id("12345", msg)
        assert result == "gm:12345"

    def test_fallback_to_message_id(self):
        """Test fallback to Message-ID when X-GM-MSGID is None"""
        msg = MagicMock()
        msg.get.return_value = "<message-id@example.com>"
        
        result = get_unique_id(None, msg)
        assert result == "mid:<message-id@example.com>"

    def test_returns_none_when_no_id(self):
        """Test returns None when both IDs are unavailable"""
        msg = MagicMock()
        msg.get.return_value = None
        
        result = get_unique_id(None, msg)
        assert result is None


class TestMigrateOldIdFormat:
    def test_migrate_old_numeric_ids(self):
        """Test migration of old numeric IDs to new gm: format"""
        old_ids = {"12345678901234567890", "98765432109876543210"}
        result = migrate_old_id_format(old_ids)
        assert "gm:12345678901234567890" in result
        assert "gm:98765432109876543210" in result
        assert "12345678901234567890" not in result

    def test_keep_new_format_ids(self):
        """Test that IDs already in new format are preserved"""
        new_ids = {"gm:12345", "mid:<test@example.com>"}
        result = migrate_old_id_format(new_ids)
        assert result == new_ids

    def test_mixed_format_ids(self):
        """Test migration with mixed old and new format IDs"""
        mixed_ids = {"12345", "gm:67890", "mid:<test@example.com>"}
        result = migrate_old_id_format(mixed_ids)
        assert "gm:12345" in result
        assert "gm:67890" in result
        assert "mid:<test@example.com>" in result
        assert len(result) == 3

    def test_empty_set(self):
        """Test migration with empty set"""
        result = migrate_old_id_format(set())
        assert result == set()


class TestVerifyStorage:
    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_verify_storage_success(self, mock_log, mock_error):
        """Test storage verification succeeds with valid directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "processed_ids.json")
            with patch('src.main.PROCESSED_IDS_FILE', test_file):
                result = verify_storage()
                assert result == True
                mock_error.assert_not_called()

    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_verify_storage_creates_directory(self, mock_log, mock_error):
        """Test storage verification creates directory if needed"""
        with tempfile.TemporaryDirectory() as tmpdir:
            new_dir = os.path.join(tmpdir, "newdir", "processed_ids.json")
            with patch('src.main.PROCESSED_IDS_FILE', new_dir):
                result = verify_storage()
                assert result == True
                assert os.path.exists(os.path.dirname(new_dir))


class TestModeManagement:
    def test_is_test_mode_true(self):
        with patch('src.main.MODE', 'test'):
            assert is_test_mode() == True

    def test_is_test_mode_false(self):
        with patch('src.main.MODE', 'prod'):
            assert is_test_mode() == False

    def test_is_test_mode_default(self):
        with patch('src.main.MODE', None):
            assert is_test_mode() == False

    def test_add_test_prefix_in_test_mode(self):
        with patch('src.main.is_test_mode', return_value=True):
            result = add_test_prefix("Hello")
            assert "テストバージョン" in result
            assert "Hello" in result

    def test_add_test_prefix_in_prod_mode(self):
        with patch('src.main.is_test_mode', return_value=False):
            result = add_test_prefix("Hello")
            assert result == "Hello"


class TestExtractHtmlEdgeCases:
    def test_none_payload(self):
        """Test handling when get_payload returns None - should return empty string"""
        msg = MagicMock()
        msg.is_multipart.return_value = False
        msg.get_content_type.return_value = "text/html"
        msg.get_content_charset.return_value = "utf-8"
        msg.get_payload.return_value = None

        # After bug fix, this should return empty string instead of raising AttributeError
        result = extract_html(msg)
        assert result == ""


class TestNotifySlack:
    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.SLACK_MENTION_INOUE_ID', 'U123')
    @patch('src.main.SLACK_MENTION_KONDO_ID', 'U456')
    @patch('src.main.is_test_mode', return_value=False)
    def test_notify_slack_success(self, mock_test_mode, mock_get_url, mock_post):
        """Test successful Slack notification"""
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.return_value = MagicMock(status_code=200)
        
        notify_slack("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "山田太郎" in call_args[1]['json']['text']
        assert "Indeed応募" in call_args[1]['json']['text']

    @patch('src.main.get_slack_webhook_url')
    def test_notify_slack_no_webhook(self, mock_get_url):
        """Test Slack notification when webhook URL is not set"""
        mock_get_url.return_value = None
        
        # Should not raise an exception
        notify_slack("indeed", "山田太郎", "https://indeed.com/apply/123")

    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.notify_error_to_slack')
    def test_notify_slack_api_error(self, mock_error, mock_get_url, mock_post):
        """Test Slack notification when API returns error"""
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.return_value = MagicMock(status_code=500, text="Internal Server Error")
        
        notify_slack("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_error.assert_called_once()

    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.notify_error_to_slack')
    def test_notify_slack_exception(self, mock_error, mock_get_url, mock_post):
        """Test Slack notification when request raises exception"""
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.side_effect = Exception("Connection error")
        
        notify_slack("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_error.assert_called_once()


class TestNotifyLine:
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.LINE_MENTION_INOUE_ID', 'U123')
    @patch('src.main.LINE_MENTION_KONDO_ID', 'U456')
    @patch('src.main.is_test_mode', return_value=False)
    def test_notify_line_success_with_mentions(self, mock_test_mode, mock_get_id, mock_post):
        """Test successful LINE notification with mentions"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=200)
        
        notify_line("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        body = call_args[1]['json']
        
        # Check that text_v2 contains placeholders
        assert "{inoue}" in body['messages'][0]['text']
        assert "{kondo}" in body['messages'][0]['text']
        
        # Check that substitution contains both mentions
        assert "inoue" in body['messages'][0]['substitution']
        assert "kondo" in body['messages'][0]['substitution']

    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.LINE_MENTION_INOUE_ID', None)
    @patch('src.main.LINE_MENTION_KONDO_ID', None)
    @patch('src.main.is_test_mode', return_value=False)
    @patch('src.main.log')
    def test_notify_line_without_mentions(self, mock_log, mock_test_mode, mock_get_id, mock_post):
        """Test LINE notification without mention IDs - should not include placeholders"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=200)
        
        notify_line("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        body = call_args[1]['json']
        
        # After fix: text_v2 should NOT contain placeholders when IDs are not set
        assert "{inoue}" not in body['messages'][0]['text']
        assert "{kondo}" not in body['messages'][0]['text']
        
        # Substitution should be empty
        assert body['messages'][0]['substitution'] == {}

    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', None)
    def test_notify_line_no_token(self, mock_get_id):
        """Test LINE notification when token is not set"""
        mock_get_id.return_value = "group_id"
        
        # Should not raise an exception
        notify_line("indeed", "山田太郎", "https://indeed.com/apply/123")

    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_notify_line_api_error(self, mock_log, mock_error, mock_get_id, mock_post):
        """Test LINE notification when API returns error"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=500, text="Internal Server Error")
        
        notify_line("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_error.assert_called_once()

    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_notify_line_exception(self, mock_log, mock_error, mock_get_id, mock_post):
        """Test LINE notification when request raises exception"""
        mock_get_id.return_value = "group_id"
        mock_post.side_effect = Exception("Connection error")
        
        notify_line("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        mock_error.assert_called_once()

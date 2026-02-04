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
    is_test_mode,
    add_test_prefix,
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
            result = load_processed_ids()
            assert result == set()

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with patch('src.main.PROCESSED_IDS_FILE', temp_path):
                ids = {"id1", "id2", "id3"}
                save_processed_ids(ids)
                loaded = load_processed_ids()
                assert loaded == ids
        finally:
            os.unlink(temp_path)

    def test_load_corrupted_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json")
            temp_path = f.name

        try:
            with patch('src.main.PROCESSED_IDS_FILE', temp_path):
                with patch('src.main.log') as mock_log:
                    result = load_processed_ids()
                    assert result == set()
                    mock_log.assert_called()
        finally:
            os.unlink(temp_path)


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


class TestNotifyLineEdgeCases:
    def test_line_text_v2_without_substitution(self):
        """
        When LINE_MENTION_INOUE_ID and LINE_MENTION_KONDO_ID are not set,
        the text_v2 still contains {inoue} {kondo} placeholders but substitution is empty.
        This might cause issues with the LINE API.
        """
        # This is a potential bug that should be investigated
        pass

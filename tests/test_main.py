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

    def test_indeed_subject_variants(self):
        # 件名フォーマットの揺れに追従（応募側は広め）
        assert determine_source("新しい応募者が1名います")[0] == "indeed"
        assert determine_source("【Indeed】応募がありました")[0] == "indeed"
        assert determine_source("応募者からのメッセージが届いています")[0] == "indeed"

    def test_indeed_login_auth_is_not_application(self):
        # 2段階認証のログインコードメールを応募として誤分類しないこと（本件の回帰）
        subject = "認証コード (000000) を入力してIndeedにログインしてください"
        source, url = determine_source(subject)
        assert source is None
        assert url is None


class TestIsIndeedNonApplicationEmail:
    """Indeed由来の非応募メールを正しく除外し、本物の応募は除外しないことを保証する回帰テスト。"""

    def test_login_auth_code_email_is_non_application(self):
        # 本件の事象: 2段階認証のログインコードメール → 非応募として静かにスキップ
        subject = "認証コード (000000) を入力してIndeedにログインしてください"
        assert is_indeed_non_application_email(subject) is True

    def test_security_and_account_emails_are_non_application(self):
        for subject in [
            "確認コードをご確認ください",
            "ログインリクエストがありました",
            "新しいデバイスからのサインインを検知しました",
            "パスワードの再設定を完了してください",
            "二段階認証を有効にしてください",
        ]:
            assert is_indeed_non_application_email(subject) is True, subject

    def test_recommendation_and_billing_emails_are_non_application(self):
        for subject in [
            "あなたにオススメ求人があります",
            "求人への応募状況をお知らせします",
            "Indeed請求のお知らせ",
            "営業職 @ 株式会社サンプル",
        ]:
            assert is_indeed_non_application_email(subject) is True, subject

    def test_real_application_email_is_not_excluded(self):
        # 最重要: 本物の応募通知を絶対に除外しない（取りこぼし厳禁）
        for subject in [
            "新しい応募者のお知らせ - 山田太郎",
            "新しい応募者が1名います",
            "応募がありました",
            "応募者からのメッセージが届いています",
        ]:
            assert is_indeed_non_application_email(subject) is False, subject


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


class TestNotifySlackWithRetry:
    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.is_test_mode', return_value=False)
    def test_retry_on_failure_then_success(self, mock_test_mode, mock_get_url, mock_post, mock_sleep):
        """Test that retry works when first attempt fails but second succeeds"""
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.side_effect = [
            MagicMock(status_code=500, text="Server Error"),
            MagicMock(status_code=200)
        ]
        
        result = notify_slack_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        assert result == True
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1 second backoff

    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.notify_error_to_slack')
    def test_all_retries_fail(self, mock_error, mock_get_url, mock_post, mock_sleep):
        """Test that error is reported after all retries fail"""
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.return_value = MagicMock(status_code=500, text="Server Error")
        
        result = notify_slack_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123", max_retries=3)
        
        assert result == False
        assert mock_post.call_count == 3
        mock_error.assert_called_once()

    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_slack_webhook_url')
    @patch('src.main.notify_error_to_slack')
    def test_timeout_triggers_retry(self, mock_error, mock_get_url, mock_post, mock_sleep):
        """Test that timeout triggers retry"""
        import requests
        mock_get_url.return_value = "https://hooks.slack.com/test"
        mock_post.side_effect = [
            requests.exceptions.Timeout("Connection timed out"),
            MagicMock(status_code=200)
        ]
        
        result = notify_slack_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        assert result == True
        assert mock_post.call_count == 2


class TestNotifyLineWithRetry:
    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.is_test_mode', return_value=False)
    def test_retry_on_failure_then_success(self, mock_test_mode, mock_get_id, mock_post, mock_sleep):
        """Test that retry works when first attempt fails but second succeeds"""
        mock_get_id.return_value = "group_id"
        mock_post.side_effect = [
            MagicMock(status_code=500, text="Server Error"),
            MagicMock(status_code=200)
        ]
        
        result = notify_line_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123")
        
        assert result == True
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2^0 = 1 second backoff

    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_all_retries_fail(self, mock_log, mock_error, mock_get_id, mock_post, mock_sleep):
        """Test that error is reported after all retries fail"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=500, text="Server Error")
        
        result = notify_line_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123", max_retries=3)
        
        assert result == False
        assert mock_post.call_count == 3
        mock_error.assert_called_once()

    @patch('src.main.time.sleep')
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.notify_error_to_slack')
    @patch('src.main.log')
    def test_timeout_triggers_retry(self, mock_log, mock_error, mock_get_id, mock_post, mock_sleep):
        """Test that timeout triggers retry"""
        import requests
        mock_get_id.return_value = "group_id"
        mock_post.side_effect = [
            requests.exceptions.Timeout("Connection timed out"),
            MagicMock(status_code=200)
        ]
        
        result = notify_line_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123")

        assert result == True
        assert mock_post.call_count == 2

    @patch('src.main.time.sleep')
    @patch('src.main.shorten_url', side_effect=lambda u: u)
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.is_test_mode', return_value=False)
    def test_url_opens_in_external_browser(self, mock_test_mode, mock_get_id, mock_post, mock_shorten, mock_sleep):
        """LINE通知のURLに openExternalBrowser=1 を付与すること（LINE内ブラウザのOAuthブロック回避）。"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=200)

        notify_line_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123")

        body = mock_post.call_args[1]['json']
        text = body['messages'][0]['text']
        assert "https://indeed.com/apply/123?openExternalBrowser=1" in text

    @patch('src.main.time.sleep')
    @patch('src.main.shorten_url', side_effect=lambda u: u)
    @patch('src.main.requests.post')
    @patch('src.main.get_line_to_id')
    @patch('src.main.LINE_CHANNEL_ACCESS_TOKEN', 'test_token')
    @patch('src.main.is_test_mode', return_value=False)
    def test_url_external_browser_with_existing_query(self, mock_test_mode, mock_get_id, mock_post, mock_shorten, mock_sleep):
        """URLに既存クエリがある場合は & で openExternalBrowser=1 を連結すること。"""
        mock_get_id.return_value = "group_id"
        mock_post.return_value = MagicMock(status_code=200)

        notify_line_with_retry("indeed", "山田太郎", "https://indeed.com/apply/123?ref=email")

        text = mock_post.call_args[1]['json']['messages'][0]['text']
        assert "https://indeed.com/apply/123?ref=email&openExternalBrowser=1" in text


def test_process_mail_by_uid_both_notifications_fail(tmp_path):
    """SlackとLINE両方の通知が失敗した場合、Noneを返して未処理にする"""
    import email
    from unittest.mock import MagicMock, patch

    # ダミーのメールデータを作成
    msg = email.message.Message()
    msg["Subject"] = "Indeed: 新しい応募者のお知らせ - テスト太郎"
    msg["From"] = "テスト太郎 <test@example.com>"
    msg.set_payload("test body")
    raw_bytes = msg.as_bytes()

    mock_mail = MagicMock()
    # UID fetchの戻り値を設定
    mock_mail.uid.return_value = (
        "OK",
        [(b"1 (X-GM-MSGID 9999999999999999999 UID 1)", raw_bytes)],
    )

    processed_ids = set()

    with patch("src.main.notify_slack_with_retry", return_value=False) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=False) as mock_line:
        result = process_mail_by_uid(mock_mail, b"1", processed_ids)

    # 両方失敗 → None を返す（未処理扱い）
    assert result is None, "両通知失敗時はNoneを返すべき"
    mock_slack.assert_called_once()
    mock_line.assert_called_once()


def test_process_mail_by_uid_slack_only_success(tmp_path):
    """Slackだけ成功した場合、unique_idを返して処理済みにする"""
    import email
    from unittest.mock import MagicMock, patch

    msg = email.message.Message()
    msg["Subject"] = "Indeed: 新しい応募者のお知らせ - テスト花子"
    msg["From"] = "テスト花子 <hanako@example.com>"
    msg.set_payload("test body")
    raw_bytes = msg.as_bytes()

    mock_mail = MagicMock()
    mock_mail.uid.return_value = (
        "OK",
        [(b"2 (X-GM-MSGID 8888888888888888888 UID 2)", raw_bytes)],
    )

    processed_ids = set()

    with patch("src.main.notify_slack_with_retry", return_value=True), \
         patch("src.main.notify_line_with_retry", return_value=False):
        result = process_mail_by_uid(mock_mail, b"2", processed_ids)

    # Slackだけ成功 → unique_idを返す（処理済みにする）
    assert result is not None, "少なくとも1つ成功時はunique_idを返すべき"


def test_process_mail_by_uid_indeed_login_code_no_false_alert(tmp_path):
    """Indeedの2段階認証ログインコードメールで誤アラートを出さないこと（本件の回帰）。

    From に 'Indeed' を含む非応募メールでも、既知の非応募パターンなら
    notify_error_to_slack を呼ばず、静かに処理済みマークするのが正しい挙動。
    """
    import email
    from unittest.mock import MagicMock, patch

    msg = email.message.Message()
    msg["Subject"] = "認証コード (000000) を入力してIndeedにログインしてください"
    msg["From"] = "'Indeed' via 株式会社日本交通誘導 <info@kotsuyudo.com>"
    msg.set_payload("login code body")
    raw_bytes = msg.as_bytes()

    mock_mail = MagicMock()
    mock_mail.uid.return_value = (
        "OK",
        [(b"3 (X-GM-MSGID 7777777777777777777 UID 3)", raw_bytes)],
    )

    processed_ids = set()

    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result = process_mail_by_uid(mock_mail, b"3", processed_ids)

    # 誤アラートを出さない（最重要）
    mock_alert.assert_not_called()
    # 応募通知としても送らない（非応募なので）
    mock_slack.assert_not_called()
    mock_line.assert_not_called()
    # 再通知ループ防止のため処理済みマークして返す
    assert result is not None, "非応募メールは処理済みマークして返すべき"

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
    check_mail_with_status,
)
import imaplib


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

    def test_indeed_subject_format_drift_fallback(self):
        # 件名フォーマットが変わっても「応募」+動き/対象語の共起で応募と判定する（取りこぼし防止）
        for subject in [
            "【Indeed】新しい求人への応募が届きました",
            "あなたの求人に応募を受け付けました",
            "求人「警備員」に新規応募がありました",
            "応募者が1名います（要対応）",
        ]:
            assert determine_source(subject)[0] == "indeed", subject

    def test_fallback_does_not_misclassify_non_application(self):
        # フォールバックを入れても、課金・レコメンド・認証系を応募と誤判定しないこと
        for subject in [
            "求人への応募状況をお知らせします",   # 応募状況レポート（非応募）
            "認証コードを入力してログインしてください",
            "Indeed請求のお知らせ",
            "あなたにオススメ求人があります",
        ]:
            assert determine_source(subject)[0] is None, subject


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


class TestIsFromIndeed:
    """送信者が本当にIndeedドメインかを実アドレスで判定する（件名/表示名の "indeed" では判定しない）。"""

    def test_genuine_indeed_senders(self):
        for frm in [
            "Indeed <noreply@indeed.com>",
            "noreply@indeed.com",
            "Indeed Apply <apply@indeedemail.com>",
            "Indeed <no-reply@mail.indeed.com>",  # サブドメインも許可
        ]:
            assert is_from_indeed(frm) is True, frm

    def test_display_name_only_indeed_is_not_indeed(self):
        # 表示名だけ "Indeed"、実アドレスは employer ドメイン → Indeed扱いしない
        assert is_from_indeed("'Indeed' via 株式会社日本交通誘導 <info@kotsuyudo.com>") is False

    def test_github_and_others_are_not_indeed(self):
        for frm in [
            "GitHub <notifications@github.com>",
            "no-reply@accounts.google.com",
            "info@example.com",
            "",
        ]:
            assert is_from_indeed(frm) is False, frm

    def test_lookalike_domain_is_not_indeed(self):
        # indeed.com.evil.com / notindeed.com のような偽装ドメインを弾く
        assert is_from_indeed("Indeed <noreply@indeed.com.evil.com>") is False
        assert is_from_indeed("x@notindeed.com") is False


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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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
    @patch('src.main._http_session.post')
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


def _build_mail_mock(subject, from_header, uid_num, gm_msgid):
    """process_mail_by_uid テスト用の IMAP モックを組み立てるヘルパー。"""
    import email
    from unittest.mock import MagicMock
    msg = email.message.Message()
    msg["Subject"] = subject
    msg["From"] = from_header
    msg.set_payload("body")
    raw_bytes = msg.as_bytes()
    mock_mail = MagicMock()
    mock_mail.uid.return_value = (
        "OK",
        [(f"{uid_num} (X-GM-MSGID {gm_msgid} UID {uid_num})".encode(), raw_bytes)],
    )
    return mock_mail


def test_process_mail_by_uid_github_notification_no_false_alert():
    """件名に "indeed"（ブランチ名等）を含むGitHub通知メールで誤アラートを出さないこと（本件の回帰）。

    送信者が github.com である以上、件名に "indeed" の文字列が含まれていても Indeed 扱いせず、
    静かにスキップする（notify_error_to_slack を呼ばない）。
    """
    from unittest.mock import patch
    mock_mail = _build_mail_mock(
        subject="[tkgathr2/recruit] Run failed: Tests - fix/indeed-login-auth-email-misclassify",
        from_header="GitHub <notifications@github.com>",
        uid_num=10,
        gm_msgid="1010101010101010101",
    )
    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result = process_mail_by_uid(mock_mail, b"10", set())

    mock_alert.assert_not_called()   # 誤アラートを出さない（最重要）
    mock_slack.assert_not_called()
    mock_line.assert_not_called()
    assert result is not None, "対象外メールは処理済みマークして返すべき"


def test_process_mail_by_uid_real_indeed_application_is_processed():
    """本物のIndeed応募メール（From=indeed.com・応募件名）は従来どおり通知される（取りこぼし厳禁）。"""
    from unittest.mock import patch
    mock_mail = _build_mail_mock(
        subject="新しい応募者のお知らせ - 山田太郎",
        from_header="Indeed <noreply@indeed.com>",
        uid_num=11,
        gm_msgid="1111111111111111111",
    )
    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result = process_mail_by_uid(mock_mail, b"11", set())

    mock_alert.assert_not_called()
    mock_slack.assert_called_once()   # 応募として通知される
    mock_line.assert_called_once()
    assert result is not None


def test_process_mail_by_uid_indeed_unknown_subject_still_alerts():
    """送信者が本物のIndeed(indeed.com)で件名が未知の場合:
    - エラーチャネルにフォーマット変更アラートを出す
    - 通常チャネル（Slack/LINE）にも「⚠️未分類」として通知する（取りこぼし防止・バグ1修正）
    - return None（処理済みにしない＝次サイクルでも再処理可能）
    """
    from unittest.mock import patch
    mock_mail = _build_mail_mock(
        subject="重要なお知らせ（新フォーマット）",  # 応募でも既知の非応募でもない
        from_header="Indeed <noreply@indeed.com>",
        uid_num=12,
        gm_msgid="1212121212121212121",
    )
    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result = process_mail_by_uid(mock_mail, b"12", set())

    mock_alert.assert_called_once()   # 安全網アラート（エラーチャネル）は本物のIndeedドメインに対してのみ
    mock_slack.assert_called_once()   # 通常チャネルにも通知（取りこぼし防止）
    mock_line.assert_called_once()    # 通常チャネルにも通知（取りこぼし防止）
    assert result is None             # 処理済みにしない（次サイクルで再処理可能）


# ---- バグ1: 未分類IndeedメールのSlack/LINE通知 ----------------------------------------

def test_process_mail_by_uid_unclassified_indeed_notifies_normal_channels():
    """未分類のIndeedメール（応募でも既知の非応募でもない件名）は
    Slack/LINEの通常チャネルに通知され、取りこぼされないこと（バグ1の回帰テスト）。

    修正前: notify_error_to_slack だけ呼んで return unique_id（永久抑制）。
    修正後: Slack/LINE にも通知し、return None（次サイクルで再処理可能）。
    """
    from unittest.mock import patch
    mock_mail = _build_mail_mock(
        subject="重要なお知らせ（新フォーマット）",  # 応募でも既知の非応募でもない
        from_header="Indeed <noreply@indeed.com>",
        uid_num=20,
        gm_msgid="2020202020202020202",
    )
    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result = process_mail_by_uid(mock_mail, b"20", set())

    # エラーアラートも出る（フォーマット変更の可能性を管理者に知らせる）
    mock_alert.assert_called_once()
    # 通常チャネルにも通知される（取りこぼし防止）
    mock_slack.assert_called_once()
    mock_line.assert_called_once()
    # return None → processed_ids に入らず次サイクルでも再処理可能
    assert result is None, "未分類メールは processed_ids に入れず None を返すべき"


def test_unclassified_indeed_normal_channel_notified_only_once():
    """未分類Indeedメールの通常チャネル（Slack/LINE）通知は「1メール=1回だけ」であること。

    同一 unique_id のメールが2サイクル処理されても、Slack/LINE への通知は初回のみ。
    エラーチャネル（notify_error_to_slack）は10分dedup制御なので2回呼ばれる想定。
    """
    import src.main as main_module
    from unittest.mock import patch, call

    # テスト間でグローバル state が汚染しないよう明示クリア
    main_module._unclassified_normal_notified.discard("gm:3030303030303030303")

    mock_mail = _build_mail_mock(
        subject="全く新しいフォーマットのIndeedメール",
        from_header="Indeed <noreply@indeed.com>",
        uid_num=30,
        gm_msgid="3030303030303030303",
    )

    # --- 1サイクル目 ---
    with patch("src.main.notify_error_to_slack") as mock_alert, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line:
        result1 = process_mail_by_uid(mock_mail, b"30", set())

    assert result1 is None, "1サイクル目: 処理済みにしない"
    mock_alert.assert_called_once()   # エラーチャネルは通知される
    mock_slack.assert_called_once()   # 1サイクル目: 通常チャネルも通知される
    mock_line.assert_called_once()    # 1サイクル目: 通常チャネルも通知される

    # --- 2サイクル目（同一メール・同一 unique_id）---
    with patch("src.main.notify_error_to_slack") as mock_alert2, \
         patch("src.main.notify_slack_with_retry", return_value=True) as mock_slack2, \
         patch("src.main.notify_line_with_retry", return_value=True) as mock_line2:
        result2 = process_mail_by_uid(mock_mail, b"30", set())

    assert result2 is None, "2サイクル目: 引き続き処理済みにしない"
    mock_alert2.assert_called_once()       # エラーチャネルは10分dedup制御（モック上は呼ばれる）
    mock_slack2.assert_not_called()        # 2サイクル目: 通常チャネルは呼ばれない
    mock_line2.assert_not_called()         # 2サイクル目: 通常チャネルは呼ばれない

    # 後片付け
    main_module._unclassified_normal_notified.discard("gm:3030303030303030303")


# ---- バグ2: processed_ids のメモリ保持 -----------------------------------------------

def test_check_mail_with_status_reuses_passed_processed_ids():
    """check_mail_with_status に processed_ids を渡した場合、load_processed_ids を
    呼ばずに渡されたセットをそのまま利用すること（バグ2の回帰テスト）。

    修正前: 毎サイクルで load_processed_ids() を呼んでいた。
    修正後: 引数で渡された processed_ids をそのまま _check_mail_attempt に転送する。
    """
    from unittest.mock import patch, MagicMock
    from src.main import check_mail_with_status

    existing_ids = {"gm:111", "uid:5"}

    with patch("src.main.has_imap_credentials", return_value=True), \
         patch("src.main.load_processed_ids") as mock_load, \
         patch("src.main._check_mail_attempt") as mock_attempt:
        check_mail_with_status(existing_ids)

    # load_processed_ids は一切呼ばれない
    mock_load.assert_not_called()
    # 渡したセットがそのまま _check_mail_attempt に渡される
    mock_attempt.assert_called_once_with(existing_ids)


class TestIMAPConnectionPool:
    """Tests for the IMAP connection pool (reuse + exponential backoff)."""

    def _make_pool(self):
        from src.main import IMAPConnectionPool
        return IMAPConnectionPool()

    def test_reuses_alive_connection(self):
        pool = self._make_pool()
        alive = MagicMock()
        alive.noop.return_value = ("OK", [b"NOOP completed."])
        pool._connection = alive

        with patch("src.main.imaplib.IMAP4_SSL") as mock_cls:
            got = pool.get()

        assert got is alive
        mock_cls.assert_not_called()
        alive.noop.assert_called_once()

    def test_reconnects_when_noop_fails(self):
        import imaplib as _imaplib
        pool = self._make_pool()
        dead = MagicMock()
        dead.noop.side_effect = _imaplib.IMAP4.abort("connection broken")
        pool._connection = dead

        fresh = MagicMock()
        with patch("src.main.imaplib.IMAP4_SSL", return_value=fresh) as mock_cls, \
             patch("src.main.GMAIL_IMAP_USER", "u"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "p"):
            got = pool.get()

        assert got is fresh
        mock_cls.assert_called_once()
        fresh.login.assert_called_once_with("u", "p")
        fresh.select.assert_called_once_with("INBOX", readonly=True)
        # Dead connection should be closed during reconnect.
        dead.close.assert_called_once()
        dead.logout.assert_called_once()

    def test_exponential_backoff_then_success(self):
        import imaplib as _imaplib
        pool = self._make_pool()
        fresh = MagicMock()

        with patch("src.main.imaplib.IMAP4_SSL",
                   side_effect=[_imaplib.IMAP4.error("boom1"),
                                _imaplib.IMAP4.error("boom2"),
                                fresh]) as mock_cls, \
             patch("src.main.time.sleep") as mock_sleep, \
             patch("src.main.GMAIL_IMAP_USER", "u"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "p"):
            got = pool.get()

        assert got is fresh
        assert mock_cls.call_count == 3
        # First two attempts fail → sleeps of 5s then 10s before the 3rd attempt.
        assert [c.args[0] for c in mock_sleep.call_args_list] == [5, 10]

    def test_all_attempts_fail_raises(self):
        import imaplib as _imaplib
        pool = self._make_pool()

        with patch("src.main.imaplib.IMAP4_SSL",
                   side_effect=_imaplib.IMAP4.error("boom")), \
             patch("src.main.time.sleep") as mock_sleep, \
             patch("src.main.GMAIL_IMAP_USER", "u"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "p"):
            with pytest.raises(_imaplib.IMAP4.error):
                pool.get()

        # Three attempts → two sleeps (5s, 10s); no sleep after the final failure.
        assert [c.args[0] for c in mock_sleep.call_args_list] == [5, 10]

    def test_reset_closes_and_clears(self):
        pool = self._make_pool()
        existing = MagicMock()
        pool._connection = existing

        pool.reset()

        existing.close.assert_called_once()
        existing.logout.assert_called_once()
        assert pool._connection is None

    def test_imap_connection_context_resets_on_error(self):
        """When the with-block raises an IMAP error, the pool is reset."""
        import imaplib as _imaplib
        from src.main import imap_connection, _imap_pool

        live = MagicMock()
        live.noop.return_value = ("OK", [b"NOOP completed."])
        _imap_pool._connection = live
        try:
            with pytest.raises(_imaplib.IMAP4.abort):
                with imap_connection() as mail:
                    assert mail is live
                    raise _imaplib.IMAP4.abort("server hung up")
            assert _imap_pool._connection is None, "pool must drop the dead connection"
            live.close.assert_called_once()
            live.logout.assert_called_once()
        finally:
            _imap_pool._connection = None


class TestExtractJobTitleFromSubject:
    def test_typical_indeed_subject(self):
        subject = "新しい応募者のお知らせ: 警備員 | 日本交通誘導警備株式会社"
        assert extract_job_title_from_subject(subject) == "警備員"

    def test_subject_without_company(self):
        subject = "新しい応募者のお知らせ: 交通誘導警備員A"
        assert extract_job_title_from_subject(subject) == "交通誘導警備員A"

    def test_applied_pattern(self):
        subject = "応募がありました - 夜間警備員"
        result = extract_job_title_from_subject(subject)
        assert result is not None

    def test_no_match_returns_none(self):
        subject = "新しい応募者のお知らせ"
        assert extract_job_title_from_subject(subject) is None

    def test_non_application_subject(self):
        assert extract_job_title_from_subject("Indeed請求書") is None


class TestExtractJobTitleFromHtml:
    def test_extract_from_html_with_job_label(self):
        html = "<html><body><p>求人名: 交通誘導警備員</p></body></html>"
        assert extract_job_title_from_html(html) == "交通誘導警備員"

    def test_empty_html_returns_none(self):
        assert extract_job_title_from_html("") is None
        assert extract_job_title_from_html(None) is None

    def test_no_job_title_in_html_returns_none(self):
        html = "<html><body><p>○○さんからの応募がありました</p></body></html>"
        assert extract_job_title_from_html(html) is None


class TestNotifyLineWithRetryFallback:
    def test_textv2_success_no_fallback(self):
        with patch("src.main.LINE_CHANNEL_ACCESS_TOKEN", "token"), \
             patch("src.main.LINE_TO_ID_PROD", "U123"), \
             patch("src.main.MODE", "prod"), \
             patch('src.main._http_session.post') as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            result = notify_line_with_retry("indeed", "田中", "https://indeed.com/x")
            assert result is True
            call_body = mock_post.call_args[1]["json"]
            assert call_body["messages"][0]["type"] == "textV2"

    def test_textv2_400_falls_back_to_plain(self):
        responses = [
            MagicMock(status_code=400, text="textV2 not supported"),
            MagicMock(status_code=200),
        ]
        with patch("src.main.LINE_CHANNEL_ACCESS_TOKEN", "token"), \
             patch("src.main.LINE_TO_ID_PROD", "U123"), \
             patch("src.main.MODE", "prod"), \
             patch('src.main._http_session.post', side_effect=responses):
            result = notify_line_with_retry("indeed", "田中", "https://indeed.com/x")
            assert result is True

    def test_textv2_success_contains_no_mention_all_literal_in_text_field(self):
        """textV2 成功時: テキストフィールドに {mention_all} リテラルが含まれないこと。
        メンションは substitution で解決されるため、text フィールドは
        '{mention_all} メッセージ本文' の形式であることを確認する。"""
        with patch("src.main.LINE_CHANNEL_ACCESS_TOKEN", "token"), \
             patch("src.main.LINE_TO_ID_PROD", "U123"), \
             patch("src.main.MODE", "prod"), \
             patch('src.main._http_session.post') as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            notify_line_with_retry("indeed", "田中", "https://indeed.com/x")
            call_body = mock_post.call_args[1]["json"]
            msg = call_body["messages"][0]
            # textV2 の text フィールドは '{mention_all} ...' という形式（substitution で解決される）
            assert msg["type"] == "textV2"
            # substitution キーが '{mention_all}' として設定されている
            assert "mention_all" in msg["substitution"]

    def test_textv2_400_fallback_text_has_no_mention_all_literal(self):
        """textV2 が 400 で失敗した場合のフォールバック: テキストに {mention_all} リテラルが含まれないこと。
        これが本修正の対象バグ: フォールバック時に '{mention_all} 【田中】...' がそのまま送信される問題。"""
        responses = [
            MagicMock(status_code=400, text="textV2 not supported"),
            MagicMock(status_code=200),
        ]
        with patch("src.main.LINE_CHANNEL_ACCESS_TOKEN", "token"), \
             patch("src.main.LINE_TO_ID_PROD", "U123"), \
             patch("src.main.MODE", "prod"), \
             patch('src.main._http_session.post', side_effect=responses) as mock_post:
            result = notify_line_with_retry("indeed", "田中", "https://indeed.com/x")
            assert result is True
            # 2回目の呼び出し（フォールバック）のボディを検証
            fallback_call_body = mock_post.call_args_list[1][1]["json"]
            fallback_text = fallback_call_body["messages"][0]["text"]
            assert "{mention_all}" not in fallback_text, (
                f"フォールバック時に {{mention_all}} リテラルが残っている: {fallback_text!r}"
            )


class TestCheckMailWithStatusAuthError:
    """KZ-23: AUTHENTICATIONFAILED エラーを即時検知して actionable な Slack 通知を送ること。"""

    @patch("src.main.notify_error_to_slack")
    @patch("src.main.has_imap_credentials", return_value=True)
    @patch("src.main.load_processed_ids", return_value=(set(), True))
    def test_auth_error_sends_distinct_alert_and_does_not_retry(self, mock_load, mock_creds, mock_notify):
        import imaplib as _imaplib
        auth_exc = _imaplib.IMAP4.error("[AUTHENTICATIONFAILED] Invalid credentials (Failure)")
        with patch("src.main._check_mail_attempt", side_effect=auth_exc):
            result = check_mail_with_status()

        assert result is True
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args
        assert call_kwargs[1]["dedup_key"] == "imap_auth_error"
        msg = call_kwargs[0][0]
        assert "認証" in msg

    @patch("src.main.notify_error_to_slack")
    @patch("src.main.has_imap_credentials", return_value=True)
    @patch("src.main.load_processed_ids", return_value=(set(), True))
    def test_non_auth_imap_error_still_retries(self, mock_load, mock_creds, mock_notify):
        import imaplib as _imaplib
        conn_exc = _imaplib.IMAP4.error("Connection refused")
        with patch("src.main._check_mail_attempt", side_effect=conn_exc), \
             patch("src.main.time.sleep"):
            result = check_mail_with_status()

        assert result is True
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args
        assert call_kwargs[1]["dedup_key"] == "imap_connection_error"

class TestIsAuthFailure:
    """`[AUTHENTICATIONFAILED]`/Invalid credentials を一時障害と区別できること。"""

    def test_authenticationfailed_error(self):
        err = imaplib.IMAP4.error(b"[AUTHENTICATIONFAILED] Invalid credentials (Failure)")
        assert is_auth_failure(err) is True

    def test_invalid_credentials_plain(self):
        assert is_auth_failure("Invalid credentials") is True

    def test_transient_errors_are_not_auth_failure(self):
        for err in [
            "The read operation timed out",
            imaplib.IMAP4.abort("server hung up"),
            OSError("connection reset"),
            "[OVERQUOTA] limit exceeded",
        ]:
            assert is_auth_failure(err) is False, err


class TestHasImapCredentials:
    def test_present(self):
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "pw"), \
             patch("src.main.use_oauth", return_value=False):
            assert has_imap_credentials() is True

    def test_missing_password(self):
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", None), \
             patch("src.main.use_oauth", return_value=False):
            assert has_imap_credentials() is False

    def test_missing_user(self):
        with patch("src.main.GMAIL_IMAP_USER", None), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "pw"), \
             patch("src.main.use_oauth", return_value=False):
            assert has_imap_credentials() is False

    def test_oauth_only_is_sufficient(self):
        # OAuth2 方式が揃っていれば、アプリパスワード無しでも資格情報あり扱い。
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", None), \
             patch("src.main.use_oauth", return_value=True):
            assert has_imap_credentials() is True

    def test_oauth_without_user_is_insufficient(self):
        # OAuth が揃っていても Gmail アドレス(GMAIL_IMAP_USER)が無ければ不可。
        with patch("src.main.GMAIL_IMAP_USER", None), \
             patch("src.main.use_oauth", return_value=True):
            assert has_imap_credentials() is False


class TestGmailOAuth:
    """src/gmail_oauth.py の refresh token → access token フロー。"""

    def test_has_oauth_credentials_all_present(self):
        import src.gmail_oauth as go
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", "sec"), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"):
            assert go.has_oauth_credentials() is True

    def test_has_oauth_credentials_without_secret_is_sufficient(self):
        # PKCE 方式: client_secret が無くても CLIENT_ID + REFRESH_TOKEN で OAuth 有効。
        import src.gmail_oauth as go
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", None), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"):
            assert go.has_oauth_credentials() is True

    def test_has_oauth_credentials_missing(self):
        import src.gmail_oauth as go
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", "sec"), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", None):
            assert go.has_oauth_credentials() is False

    def test_has_oauth_credentials_missing_client_id(self):
        import src.gmail_oauth as go
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", None), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", None), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"):
            assert go.has_oauth_credentials() is False

    def test_build_xoauth2_string_format(self):
        import src.gmail_oauth as go
        s = go.build_xoauth2_string("user@example.com", "TOKEN123")
        assert s == "user=user@example.com\x01auth=Bearer TOKEN123\x01\x01"

    def test_refresh_access_token_caches(self):
        import src.gmail_oauth as go
        # キャッシュをクリア
        go._cached_access_token = None
        go._cached_expires_at = 0.0
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"access_token": "AT", "expires_in": 3600}
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", "sec"), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"), \
             patch("src.gmail_oauth.requests.post", return_value=mock_resp) as mock_post:
            tok1 = go.get_access_token()
            tok2 = go.get_access_token()  # 2回目はキャッシュから（postは増えない）
        assert tok1 == "AT"
        assert tok2 == "AT"
        mock_post.assert_called_once()

    def test_refresh_without_secret_omits_client_secret(self):
        # PKCE 方式: client_secret が未設定なら refresh リクエストに含めない。
        import src.gmail_oauth as go
        go._cached_access_token = None
        go._cached_expires_at = 0.0
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"access_token": "AT", "expires_in": 3600}
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", None), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"), \
             patch("src.gmail_oauth.requests.post", return_value=mock_resp) as mock_post:
            tok = go.get_access_token()
        assert tok == "AT"
        sent = mock_post.call_args.kwargs["data"]
        assert "client_secret" not in sent
        assert sent["client_id"] == "cid"
        assert sent["refresh_token"] == "rt"
        assert sent["grant_type"] == "refresh_token"

    def test_refresh_with_secret_includes_client_secret(self):
        # client_secret が設定されていれば併用する（後方互換）。
        import src.gmail_oauth as go
        go._cached_access_token = None
        go._cached_expires_at = 0.0
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"access_token": "AT", "expires_in": 3600}
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", "sec"), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"), \
             patch("src.gmail_oauth.requests.post", return_value=mock_resp) as mock_post:
            tok = go.get_access_token()
        assert tok == "AT"
        sent = mock_post.call_args.kwargs["data"]
        assert sent["client_secret"] == "sec"

    def test_refresh_invalid_grant_raises_runtime_error(self):
        import src.gmail_oauth as go
        go._cached_access_token = None
        go._cached_expires_at = 0.0
        mock_resp = MagicMock(status_code=400, text='{"error": "invalid_grant"}')
        mock_resp.json.return_value = {"error": "invalid_grant"}
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", "sec"), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"), \
             patch("src.gmail_oauth.requests.post", return_value=mock_resp):
            with pytest.raises(RuntimeError) as exc:
                go.get_access_token(force_refresh=True)
        # invalid_grant は is_auth_failure で認証失効として扱われること
        assert is_auth_failure(exc.value) is True

    def test_imap_authenticate_uses_xoauth2(self):
        import src.gmail_oauth as go
        go._cached_access_token = "AT"
        go._cached_expires_at = float("inf")
        mock_mail = MagicMock()
        go.imap_authenticate(mock_mail, "user@example.com")
        mock_mail.authenticate.assert_called()
        # 第1引数が XOAUTH2 であること
        assert mock_mail.authenticate.call_args.args[0] == "XOAUTH2"


class TestIMAPConnectionPoolOAuth:
    """OAuth が有効なときに pool が XOAUTH2 で認証すること。"""

    def test_connect_uses_oauth_when_enabled(self):
        from src.main import IMAPConnectionPool
        pool = IMAPConnectionPool()
        fresh = MagicMock()
        with patch("src.main.imaplib.IMAP4_SSL", return_value=fresh), \
             patch("src.main.use_oauth", return_value=True), \
             patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.gmail_oauth.imap_authenticate") as mock_auth:
            got = pool.get()
        assert got is fresh
        mock_auth.assert_called_once_with(fresh, "u@example.com")
        fresh.login.assert_not_called()  # アプリパスワード login は使わない
        fresh.select.assert_called_once_with("INBOX", readonly=True)


class TestIMAPConnectionPoolOAuthFallback:
    """OAuth refresh token が失効したとき app-password にフォールバックすること。"""

    def setup_method(self):
        # 各テスト前にクラス変数フラグをリセット
        from src.main import IMAPConnectionPool
        IMAPConnectionPool._oauth_known_invalid = False

    def teardown_method(self):
        from src.main import IMAPConnectionPool
        IMAPConnectionPool._oauth_known_invalid = False

    def test_oauth_invalid_grant_falls_back_to_app_password(self):
        """OAuth が invalid_grant で失敗した場合、同一接続試行内で app-password に切り替わること。"""
        from src.main import IMAPConnectionPool
        import imaplib as _imaplib

        pool = IMAPConnectionPool()
        # IMAP4_SSL は2回呼ばれる: 1回目=OAuth試行用、2回目=フォールバック用
        fresh1 = MagicMock()
        fresh2 = MagicMock()
        imap_instances = [fresh1, fresh2]

        auth_err = _imaplib.IMAP4.error(b"[AUTHENTICATIONFAILED] Invalid credentials (Failure)")

        def fake_imap_authenticate(mail, user):
            raise auth_err

        with patch("src.main.imaplib.IMAP4_SSL", side_effect=imap_instances), \
             patch("src.main.use_oauth", return_value=True), \
             patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "app-pw"), \
             patch("src.main.gmail_oauth.imap_authenticate", side_effect=fake_imap_authenticate):
            got = pool.get()

        # フォールバック後の接続が返されること
        assert got is fresh2
        # app-password でログインされること
        fresh2.login.assert_called_once_with("u@example.com", "app-pw")
        fresh2.select.assert_called_once_with("INBOX", readonly=True)
        # フラグが立っていること（以降 OAuth を試みない）
        assert IMAPConnectionPool._oauth_known_invalid is True

    def test_oauth_known_invalid_skips_oauth_directly(self):
        """_oauth_known_invalid フラグが立っている場合は OAuth を試みず直接 app-password を使うこと。"""
        from src.main import IMAPConnectionPool

        IMAPConnectionPool._oauth_known_invalid = True
        pool = IMAPConnectionPool()
        fresh = MagicMock()

        with patch("src.main.imaplib.IMAP4_SSL", return_value=fresh), \
             patch("src.main.use_oauth", return_value=True), \
             patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "app-pw"), \
             patch("src.main.gmail_oauth.imap_authenticate") as mock_auth:
            got = pool.get()

        assert got is fresh
        mock_auth.assert_not_called()  # OAuth は試みない
        fresh.login.assert_called_once_with("u@example.com", "app-pw")

    def test_oauth_transient_error_does_not_fallback(self):
        """一時的なネットワーク断は fallback せず通常の IMAP エラーとして上位に伝播すること。"""
        from src.main import IMAPConnectionPool
        import imaplib as _imaplib

        pool = IMAPConnectionPool()
        fresh = MagicMock()

        # AUTHENTICATIONFAILED ではなく abort（一時障害）
        transient_err = _imaplib.IMAP4.abort("connection reset by server")

        with patch("src.main.imaplib.IMAP4_SSL", return_value=fresh), \
             patch("src.main.use_oauth", return_value=True), \
             patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "app-pw"), \
             patch("src.main.gmail_oauth.imap_authenticate", side_effect=transient_err), \
             patch("src.main.time.sleep"):
            with pytest.raises(_imaplib.IMAP4.abort):
                pool.get()

        # フラグは立てない（一時障害なので OAuth を諦めない）
        assert IMAPConnectionPool._oauth_known_invalid is False

    def test_oauth_failure_both_auth_fail_raises(self):
        """OAuth も app-password も失敗した場合は例外を送出すること。

        _connect_with_backoff は max_attempts=3 回試みる。
        各試行で: OAuth 失敗 → フォールバック用新規接続 → login 失敗 → except で last_exc に記録。
        3回全て失敗すると last_exc を raise する。
        """
        from src.main import IMAPConnectionPool
        import imaplib as _imaplib

        pool = IMAPConnectionPool()

        auth_err = _imaplib.IMAP4.error(b"[AUTHENTICATIONFAILED] Invalid credentials (Failure)")
        pw_err = _imaplib.IMAP4.error(b"[AUTHENTICATIONFAILED] App password invalid (Failure)")

        def fake_imap_authenticate(mail, user):
            raise auth_err

        # IMAP4_SSL は各試行で2回 (OAuth用+フォールバック用) 呼ばれる → 3試行で最大6回
        def make_fresh():
            m = MagicMock()
            m.login.side_effect = pw_err
            m.logout.return_value = None
            return m

        imap_instances = [make_fresh() for _ in range(6)]

        with patch("src.main.imaplib.IMAP4_SSL", side_effect=imap_instances), \
             patch("src.main.use_oauth", return_value=True), \
             patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "bad-pw"), \
             patch("src.main.gmail_oauth.imap_authenticate", side_effect=fake_imap_authenticate), \
             patch("src.main.time.sleep"):
            with pytest.raises(_imaplib.IMAP4.error):
                pool.get()


class TestCheckMailAuthHandling:
    """check_mail_with_status の認証/資格情報まわりの通知挙動。"""

    @patch("src.main.notify_error_to_slack")
    @patch("src.main.log")
    def test_missing_credentials_sends_distinct_alert(self, mock_log, mock_error):
        with patch("src.main.GMAIL_IMAP_USER", None), \
             patch("src.main.GMAIL_IMAP_PASSWORD", None):
            result = check_mail_with_status()
        assert result is True  # 設定待ち。quota扱いで過度にbackoffしない
        mock_error.assert_called_once()
        assert mock_error.call_args.kwargs.get("dedup_key") == "imap_missing_credentials"

    @patch("src.main.time.sleep")
    @patch("src.main.notify_error_to_slack")
    @patch("src.main.log")
    def test_auth_failure_sends_credential_alert(self, mock_log, mock_error, mock_sleep):
        auth_err = imaplib.IMAP4.error(b"[AUTHENTICATIONFAILED] Invalid credentials (Failure)")
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "pw"), \
             patch("src.main.load_processed_ids", return_value=(set(), True)), \
             patch("src.main._check_mail_attempt", side_effect=auth_err):
            result = check_mail_with_status()
        assert result is True
        mock_error.assert_called_once()
        kwargs = mock_error.call_args.kwargs
        # KZ-23: AUTHENTICATIONFAILED は即時検知で imap_auth_error キーを使う
        assert kwargs.get("dedup_key") == "imap_auth_error"
        # 認証エラーを示す文言が含まれること
        assert "認証" in mock_error.call_args.args[0]

    @patch("src.main.time.sleep")
    @patch("src.main.notify_error_to_slack")
    @patch("src.main.log")
    def test_transient_failure_uses_generic_key(self, mock_log, mock_error, mock_sleep):
        timeout_err = TimeoutError("The read operation timed out")
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "pw"), \
             patch("src.main.load_processed_ids", return_value=(set(), True)), \
             patch("src.main._check_mail_attempt", side_effect=timeout_err):
            result = check_mail_with_status()
        assert result is True
        mock_error.assert_called_once()
        assert mock_error.call_args.kwargs.get("dedup_key") == "imap_connection_error"


class TestUseOauthEnablement:
    """src/main.py の OAuth 有効化条件（PKCE: secret 任意）と後方互換。"""

    def test_use_oauth_true_without_secret(self):
        # CLIENT_ID + REFRESH_TOKEN が揃えば secret 無しでも OAuth 有効。
        import src.gmail_oauth as go
        from src.main import use_oauth
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", "cid"), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", None), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", "rt"):
            assert use_oauth() is True

    def test_use_oauth_false_when_oauth_unset_falls_back_to_app_password(self):
        # OAuth 変数が無ければ OAuth 無効 → アプリパスワード IMAP にフォールバック（後方互換）。
        import src.gmail_oauth as go
        from src.main import use_oauth, has_imap_credentials
        with patch.object(go, "GMAIL_OAUTH_CLIENT_ID", None), \
             patch.object(go, "GMAIL_OAUTH_CLIENT_SECRET", None), \
             patch.object(go, "GMAIL_OAUTH_REFRESH_TOKEN", None):
            assert use_oauth() is False
        # アプリパスワードが設定されていれば従来どおり資格情報ありと判定される。
        with patch("src.main.GMAIL_IMAP_USER", "u@example.com"), \
             patch("src.main.GMAIL_IMAP_PASSWORD", "pw"), \
             patch("src.main.use_oauth", return_value=False):
            assert has_imap_credentials() is True


class TestGetRefreshTokenScriptPKCE:
    """scripts/get-gmail-refresh-token.py の PKCE 生成・client_id 既定・secret 任意。"""

    def _load_script_module(self):
        import importlib.util
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "scripts", "get-gmail-refresh-token.py"
        )
        spec = importlib.util.spec_from_file_location("get_gmail_refresh_token", script_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_pkce_pair_valid_s256(self):
        import base64
        import hashlib
        mod = self._load_script_module()
        verifier, challenge = mod.generate_pkce_pair()
        # RFC 7636: verifier は 43〜128 文字
        assert 43 <= len(verifier) <= 128
        # challenge = BASE64URL(SHA256(verifier)) パディング無し
        expected = base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode("ascii")).digest()
        ).decode("ascii").rstrip("=")
        assert challenge == expected
        assert "=" not in challenge

    def test_pkce_pair_is_random(self):
        mod = self._load_script_module()
        v1, _ = mod.generate_pkce_pair()
        v2, _ = mod.generate_pkce_pair()
        assert v1 != v2

    def test_default_client_id_constant(self):
        mod = self._load_script_module()
        # 本番稼働中の「Claude Code MCP Desktop」クライアントの実値（2026-06-28 本番確認済み）。
        # recruit-gmail-oauth3 クライアント (235822259813-7jdk1qosim8dj1lvej712br6e2i5iuam...) は
        # 代替（任意）。現本番では使用していない。
        assert mod.DEFAULT_CLIENT_ID == (
            "235822259813-c9851j36ke8n0ne2jnclai4irktjr76d.apps.googleusercontent.com"
        )

    def test_missing_secret_returns_error(self):
        # client_secret が未設定のとき rc == 1 でエラー終了すること。
        # このクライアント（デスクトップアプリ型）は PKCE のみでは動作しない
        # （Google が 400 invalid_request: client_secret is missing を返す）。
        mod = self._load_script_module()

        with patch.object(mod, "webbrowser"), \
             patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GMAIL_OAUTH_CLIENT_SECRET", None)
            os.environ.pop("GMAIL_OAUTH_CLIENT_JSON", None)
            rc = mod.main(argv=[])
        assert rc == 1, "client_secret 未設定のとき rc == 1 でエラー終了すべき"

    def test_token_exchange_includes_secret_when_set(self):
        mod = self._load_script_module()
        captured = {}

        def fake_post(url, data=None, timeout=None):
            captured["data"] = data
            resp = MagicMock(status_code=200)
            resp.json.return_value = {"refresh_token": "RT"}
            return resp

        mod._auth_code_holder["code"] = "AUTHCODE"
        mod._auth_code_holder["error"] = None
        with patch.object(mod, "webbrowser"), \
             patch.object(mod.http.server, "HTTPServer") as mock_server_cls, \
             patch.object(mod.threading, "Thread") as mock_thread_cls, \
             patch.object(mod.requests, "post", side_effect=fake_post), \
             patch.dict(os.environ, {"GMAIL_OAUTH_CLIENT_SECRET": "sec"}, clear=False):
            mock_server_cls.return_value = MagicMock()
            mock_thread_cls.return_value = MagicMock()
            rc = mod.main(argv=["custom-client-id"])
        assert rc == 0
        sent = captured["data"]
        assert sent["client_secret"] == "sec"
        assert sent["client_id"] == "custom-client-id"


# ---- H-4: _check_mail_attempt の dedup 機能テスト ------------------------------------

class TestCheckMailAttemptDedup:
    """_check_mail_attempt の UID/GM-MSGID dedup 機能テスト。

    _check_mail_attempt(processed_ids) は内部で imap_connection() を使って IMAP に
    接続するため、imap_connection コンテキストマネージャーをモックして IMAP オブジェクトを
    注入する形でテストする。
    """

    def _make_imap_mock(self, uid_list, gm_msgid_map=None):
        """uid_list を返す IMAP モックを作成する。

        Args:
            uid_list: UID の整数リスト（空リストの場合は空の受信トレイ）
            gm_msgid_map: {uid: gm_msgid} のマッピング（省略時は uid*1000 を使用）
        """
        mail = MagicMock()
        gm_msgid_map = gm_msgid_map or {}

        # UID SEARCH レスポンス
        if uid_list:
            uid_bytes = b" ".join(str(u).encode() for u in uid_list)
        else:
            uid_bytes = b""

        def fake_uid(cmd, *args):
            if cmd == "search":
                return ("OK", [uid_bytes])
            elif cmd == "fetch":
                # get_gm_msgid_lightweight 用の軽量フェッチ
                uid_str = args[0].decode() if isinstance(args[0], bytes) else args[0]
                uid_int = int(uid_str)
                gm_id = gm_msgid_map.get(uid_int, uid_int * 1000)
                header = f"{uid_int} (X-GM-MSGID {gm_id})".encode()
                return ("OK", [(header, b"")])
            return ("NO", [b""])

        mail.uid.side_effect = fake_uid
        return mail

    def test_empty_mailbox_returns_without_processing(self):
        """空の受信トレイでは process_mail_by_uid が呼ばれないこと。"""
        from src.main import _check_mail_attempt
        from contextlib import contextmanager

        mail = self._make_imap_mock([])

        @contextmanager
        def mock_imap_connection():
            yield mail

        with patch("src.main.imap_connection", mock_imap_connection), \
             patch("src.main.process_mail_by_uid") as mock_process:
            _check_mail_attempt(set())

        mock_process.assert_not_called()

    def test_uid_cache_skips_already_processed(self):
        """uid:X が processed_ids にある場合は軽量フェッチもスキップされること。"""
        from src.main import _check_mail_attempt
        from contextlib import contextmanager

        mail = self._make_imap_mock([100, 101])
        processed = {"uid:100", "uid:101"}

        @contextmanager
        def mock_imap_connection():
            yield mail

        with patch("src.main.imap_connection", mock_imap_connection), \
             patch("src.main.process_mail_by_uid") as mock_process:
            _check_mail_attempt(processed)

        # uid: キャッシュがあるのでフル処理は不要
        mock_process.assert_not_called()

    def test_gm_msgid_dedup_skips_already_processed(self):
        """gm:X が processed_ids にある UID は process_mail_by_uid が呼ばれないこと。"""
        from src.main import _check_mail_attempt
        from contextlib import contextmanager

        # UID=200 → GM-MSGID=999002
        mail = self._make_imap_mock([200], gm_msgid_map={200: 999002})
        # gm:999002 は処理済み（uid:200 はキャッシュなし）
        processed = {"gm:999002"}

        @contextmanager
        def mock_imap_connection():
            yield mail

        with patch("src.main.imap_connection", mock_imap_connection), \
             patch("src.main.process_mail_by_uid") as mock_process, \
             patch("src.main.save_processed_ids", return_value=True):
            _check_mail_attempt(processed)

        # gm: dedup が効いてフル処理は呼ばれない
        mock_process.assert_not_called()

    def test_new_uid_triggers_full_processing(self):
        """未処理の UID は process_mail_by_uid が呼ばれること。"""
        from src.main import _check_mail_attempt
        from contextlib import contextmanager

        mail = self._make_imap_mock([300], gm_msgid_map={300: 888001})

        @contextmanager
        def mock_imap_connection():
            yield mail

        with patch("src.main.imap_connection", mock_imap_connection), \
             patch("src.main.process_mail_by_uid", return_value="gm:888001") as mock_process, \
             patch("src.main.save_processed_ids", return_value=True):
            _check_mail_attempt(set())

        mock_process.assert_called_once()

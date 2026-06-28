"""Gmail OAuth2 (refresh token) helper for IMAP XOAUTH2 authentication.

アプリパスワード(IMAP)は数週間で失効し、そのたびに人手での再発行が必要だった
（5/8・6/27 に失効再発）。OAuth2 の refresh token は原則失効しないため、
このモジュールで refresh token から短命の access token を取得し、IMAP の
XOAUTH2 認証に使う。社内標準も Google OAuth（[[feedback_google_auth_standard]]）。

設計方針:
- 標準ライブラリ + requests のみで実装（既存依存に google-auth 等を増やさない）。
- access token は失効が近づいたら自動で refresh する（プロセス内キャッシュ）。
- refresh token 方式が設定されていない場合は何もしない（呼び出し側がアプリ
  パスワード IMAP にフォールバックする＝移行期の安全網）。

必要な環境変数:
- GMAIL_OAUTH_CLIENT_ID  （本番値 = 「Claude Code MCP Desktop」クライアントのID）
- GMAIL_OAUTH_REFRESH_TOKEN
- GMAIL_OAUTH_CLIENT_SECRET  （必須。初回 token 交換・refresh 両方で使用）
  ※「PKCE 方式のため client_secret 不要」は誤り。実機確認で Google が
    `400 invalid_request: client_secret is missing` を返すことが判明。
    client_secret が設定されていれば送信し、なければ refresh を試みるが
    Google 側で弾かれる可能性がある。
（GMAIL_IMAP_USER は IMAP のユーザー＝Gmail アドレスとして既存のまま使う。
 本番値 = `atsuhiro@takagi.bz`。`recruit@takagi.bz` は存在しないため不可。）
"""
import os
import time
from typing import Optional

import requests

# Google OAuth2 token endpoint
GOOGLE_TOKEN_URI = os.getenv("GMAIL_OAUTH_TOKEN_URI", "https://oauth2.googleapis.com/token")

# Gmail 読み取り専用スコープ（最小権限）
GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"

GMAIL_OAUTH_CLIENT_ID = os.getenv("GMAIL_OAUTH_CLIENT_ID")
GMAIL_OAUTH_CLIENT_SECRET = os.getenv("GMAIL_OAUTH_CLIENT_SECRET")
GMAIL_OAUTH_REFRESH_TOKEN = os.getenv("GMAIL_OAUTH_REFRESH_TOKEN")

# access token の有効期限が切れる何秒前に再取得するか（クロックずれ・往復遅延の余裕）
_TOKEN_EXPIRY_SKEW_SECONDS = 120

# プロセス内 access token キャッシュ: (token, expires_at_unix)
_cached_access_token: Optional[str] = None
_cached_expires_at: float = 0.0


def has_oauth_credentials() -> bool:
    """OAuth2(refresh token) 方式に必要な資格情報が設定されているか。

    CLIENT_ID と REFRESH_TOKEN が揃っていれば OAuth 有効と判定する。
    client_secret は本来必須だが、refresh フェーズでは token エンドポイントへの
    リクエストを試みてから Google 側のエラーで判明するため、ここでは CLIENT_ID /
    REFRESH_TOKEN の有無のみで判定する（呼び出し側で secret 未設定のエラーを捕捉）。
    """
    return bool(GMAIL_OAUTH_CLIENT_ID and GMAIL_OAUTH_REFRESH_TOKEN)


def _refresh_access_token() -> str:
    """refresh token から新しい access token を取得して返す。

    Raises:
        RuntimeError: 資格情報未設定、または Google からのレスポンスが異常なとき。
        requests.RequestException: ネットワーク/HTTP エラー（呼び出し側でリトライ）。
    """
    if not has_oauth_credentials():
        raise RuntimeError("Gmail OAuth credentials are not fully configured")
    # client_secret は refresh フェーズでも実質必須（本番実機確認済み）。
    # 設定されていれば送信する。未設定の場合は Google 側で弾かれる可能性がある。
    refresh_data = {
        "client_id": GMAIL_OAUTH_CLIENT_ID,
        "refresh_token": GMAIL_OAUTH_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }
    if GMAIL_OAUTH_CLIENT_SECRET:
        refresh_data["client_secret"] = GMAIL_OAUTH_CLIENT_SECRET
    resp = requests.post(
        GOOGLE_TOKEN_URI,
        data=refresh_data,
        timeout=15,
    )
    if resp.status_code >= 400:
        # invalid_grant は refresh token 自体が失効/取り消された場合（要再同意）。
        try:
            _err_code = resp.json().get("error", "unknown")
        except Exception:
            _err_code = "parse_error"
        raise RuntimeError(
            f"Gmail OAuth token refresh failed (status={resp.status_code}, error={_err_code})"
        )
    payload = resp.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError(f"Gmail OAuth token response missing access_token: {payload}")
    expires_in = int(payload.get("expires_in", 3600))
    global _cached_access_token, _cached_expires_at
    _cached_access_token = access_token
    _cached_expires_at = time.time() + expires_in
    return access_token


def get_access_token(force_refresh: bool = False) -> str:
    """有効な access token を返す（必要なら refresh する）。

    Args:
        force_refresh: True なら強制的に再取得（XOAUTH2 失敗後の再試行用）。
    """
    now = time.time()
    if (
        not force_refresh
        and _cached_access_token
        and now < (_cached_expires_at - _TOKEN_EXPIRY_SKEW_SECONDS)
    ):
        return _cached_access_token
    return _refresh_access_token()


def build_xoauth2_string(user: str, access_token: str) -> str:
    """IMAP XOAUTH2 の認証文字列（base64前のSASL initial response）を組み立てる。

    フォーマット: 'user=<email>\x01auth=Bearer <token>\x01\x01'
    imaplib.authenticate に渡すコールバック用に bytes ではなく str を返す。
    """
    return f"user={user}\x01auth=Bearer {access_token}\x01\x01"


def imap_authenticate(mail, user: str) -> None:
    """与えられた IMAP4_SSL 接続に対し XOAUTH2 で認証する。

    access token が失効していた場合（AUTHENTICATE が NO を返す）に備え、
    force refresh して 1 回だけ再試行する。

    Raises:
        imaplib.IMAP4.error: 認証に失敗したとき（呼び出し側で is_auth_failure 判定）。
    """
    def _auth_object(token: str):
        xoauth2 = build_xoauth2_string(user, token)
        # imaplib は authobject の戻り値を SASL レスポンスとして使う。
        # bytes を返すと base64 エンコードして送信される。
        return lambda _challenge: xoauth2.encode("utf-8")

    try:
        access_token = get_access_token()
        mail.authenticate("XOAUTH2", _auth_object(access_token))
    except Exception as first_err:  # noqa: BLE001 - 1回だけ token を更新して再試行
        # access token が期限切れ等で弾かれた可能性 → 強制 refresh して再試行
        access_token = get_access_token(force_refresh=True)
        try:
            mail.authenticate("XOAUTH2", _auth_object(access_token))
        except Exception:
            # 再試行も失敗。元の例外情報を保ったまま再送出する。
            raise first_err

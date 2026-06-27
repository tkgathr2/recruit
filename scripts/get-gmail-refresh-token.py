#!/usr/bin/env python3
"""Gmail OAuth2 refresh token をワンタイムで取得するスクリプト（PKCE 方式）。

社長（または運用者）がブラウザで1回「許可」を押すだけで refresh token が取れる。
取得した refresh token を Railway 環境変数 GMAIL_OAUTH_REFRESH_TOKEN に入れれば
以後は恒久稼働する（access token は自動更新・refresh token は原則失効しない）。

★ client_secret は不要（PKCE 方式）
  デスクトップアプリ型 OAuth クライアントは PKCE（RFC 7636）を使えば
  client_secret 無しで authorization code 交換・refresh ができる
  （gcloud / rclone 等の CLI と同じ方式）。GCP の client_secret 取得・JSON
  ダウンロードが不要になるため、secret の壁を完全に回避できる。
  出典: Google「OAuth 2.0 for Mobile & Desktop Apps」
  https://developers.google.com/identity/protocols/oauth2/native-app
  → installed app の token 交換／refresh で client_secret は "Optional"、
    PKCE は明示的にサポート・推奨されている。

依存: 標準ライブラリ + requests（requirements.txt に既存）。google-auth 等は不要。

使い方:
  1. GCP の既定デスクトップアプリ型クライアントをそのまま使う（client_id は既定値あり）。
     別クライアントを使う場合のみ GMAIL_OAUTH_CLIENT_ID を指定する。
  2. このスクリプトを実行（引数も環境変数も不要・既定 client_id で動く）:
        python scripts/get-gmail-refresh-token.py
     別 client_id を使う場合:
        python scripts/get-gmail-refresh-token.py <client_id>
        GMAIL_OAUTH_CLIENT_ID=xxx python scripts/get-gmail-refresh-token.py
  3. 表示された URL をブラウザで開き、recruit@takagi.bz でログイン → 許可。
     ローカルサーバーが callback を受け取り refresh token を表示する。
  4. 出力された GMAIL_OAUTH_CLIENT_ID と GMAIL_OAUTH_REFRESH_TOKEN を Railway に設定する。
     （client_secret の入力も JSON ダウンロードも不要。）

注意:
- access_type=offline ＋ prompt=consent を必ず付ける（refresh token を確実に得るため）。
- OAuth 同意画面は "In production" に publish しておくこと。Testing のままだと
  refresh token が7日で失効する（[[feedback_google_auth_standard]]）。
- GMAIL_OAUTH_CLIENT_SECRET を設定した場合のみ secret も併用する（任意）。
"""
import base64
import hashlib
import http.server
import os
import secrets
import sys
import threading
import urllib.parse
import webbrowser

import requests

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"

# 既定のデスクトップアプリ型クライアント（recruit-gmail-oauth3）。
# client_secret 不要（PKCE）なので、この client_id だけで refresh token を取得できる。
DEFAULT_CLIENT_ID = "235822259813-7jdk1qosim8dj1lvej712br6e2i5iuam.apps.googleusercontent.com"

# デスクトップアプリ型クライアントで使うループバック redirect。
REDIRECT_HOST = "127.0.0.1"
REDIRECT_PORT = int(os.getenv("GMAIL_OAUTH_REDIRECT_PORT", "8765"))
REDIRECT_URI = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}/"

_auth_code_holder = {"code": None, "error": None}


def generate_pkce_pair() -> tuple:
    """PKCE の (code_verifier, code_challenge) を生成する。

    - code_verifier: 43〜128 文字の URL-safe ランダム文字列（RFC 7636 §4.1）。
    - code_challenge: S256 = BASE64URL(SHA256(code_verifier))（パディング無し）。

    Returns:
        (code_verifier, code_challenge) のタプル。
    """
    # token_urlsafe(96) は ~128 文字。base64url の "=" は含まれない。
    code_verifier = secrets.token_urlsafe(96)
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return code_verifier, code_challenge


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - http.server の規約
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        _auth_code_holder["code"] = params.get("code", [None])[0]
        _auth_code_holder["error"] = params.get("error", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if _auth_code_holder["code"]:
            body = "<h2>認証に成功しました。このタブを閉じてターミナルに戻ってください。</h2>"
        else:
            body = f"<h2>認証に失敗しました: {_auth_code_holder['error']}</h2>"
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, *args):  # サーバーログを抑制
        pass


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    # client_id は 引数 > 環境変数 > 既定値 の優先順で決める。
    client_id = (
        (argv[0].strip() if argv else "")
        or (os.getenv("GMAIL_OAUTH_CLIENT_ID") or "").strip()
        or DEFAULT_CLIENT_ID
    )
    # client_secret は任意（PKCE 方式のため不要）。設定されていれば併用する。
    client_secret = (os.getenv("GMAIL_OAUTH_CLIENT_SECRET") or "").strip()

    if not client_id:
        print("ERROR: client_id を決定できませんでした。", file=sys.stderr)
        return 1

    # PKCE ペアを生成（auth URL に code_challenge、token 交換で code_verifier を送る）。
    code_verifier, code_challenge = generate_pkce_pair()

    # ローカル callback サーバーを起動
    server = http.server.HTTPServer((REDIRECT_HOST, REDIRECT_PORT), _CallbackHandler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    auth_params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": GMAIL_READONLY_SCOPE,
        "access_type": "offline",      # refresh token を得るために必須
        "prompt": "consent",           # 毎回 refresh token を確実に返させる
        "code_challenge": code_challenge,    # PKCE（client_secret の代替）
        "code_challenge_method": "S256",
    }
    auth_url = AUTH_URI + "?" + urllib.parse.urlencode(auth_params)
    print("\n以下の URL をブラウザで開いて、recruit@takagi.bz でログイン→許可してください:\n")
    print(auth_url + "\n")
    print(f"使用する client_id: {client_id}")
    print(f"認証方式: {'PKCE + client_secret' if client_secret else 'PKCE のみ（client_secret 不要）'}\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print(f"ローカルで callback を待機中... ({REDIRECT_URI})")
    thread.join(timeout=300)
    server.server_close()

    if _auth_code_holder["error"]:
        print(f"ERROR: 認証が拒否されました: {_auth_code_holder['error']}", file=sys.stderr)
        return 1
    code = _auth_code_holder["code"]
    if not code:
        print("ERROR: 認証コードを取得できませんでした（タイムアウト）。", file=sys.stderr)
        return 1

    # 認証コード → refresh token に交換（PKCE: code_verifier を送る。secret は任意）。
    token_data = {
        "code": code,
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
        "code_verifier": code_verifier,
    }
    if client_secret:
        token_data["client_secret"] = client_secret

    resp = requests.post(TOKEN_URI, data=token_data, timeout=30)
    if resp.status_code >= 400:
        print(f"ERROR: token 交換に失敗しました (status={resp.status_code}): {resp.text}", file=sys.stderr)
        return 1
    payload = resp.json()
    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        print(
            "ERROR: refresh_token がレスポンスに含まれていません。\n"
            "（既に同意済みの場合は Google アカウントのアクセス権を一度削除してから再実行してください）\n"
            f"レスポンス: {payload}",
            file=sys.stderr,
        )
        return 1

    print("\n=== 取得成功 ===")
    print("以下を Railway の recruit → Variables に設定してください:\n")
    print(f"GMAIL_OAUTH_CLIENT_ID={client_id}")
    print(f"GMAIL_OAUTH_REFRESH_TOKEN={refresh_token}")
    if client_secret:
        print(f"GMAIL_OAUTH_CLIENT_SECRET={client_secret}")
    else:
        print("# GMAIL_OAUTH_CLIENT_SECRET は PKCE 方式のため不要")
    print("\n（GMAIL_IMAP_USER は Gmail アドレスのまま据え置きで OK）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

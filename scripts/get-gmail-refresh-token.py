#!/usr/bin/env python3
"""Gmail OAuth2 refresh token をワンタイムで取得するスクリプト。

社長（または運用者）がブラウザで1回「許可」を押すだけで refresh token が取れる。
取得した refresh token を Railway 環境変数 GMAIL_OAUTH_REFRESH_TOKEN に入れれば
以後は恒久稼働する（access token は自動更新・refresh token は原則失効しない）。

依存: 標準ライブラリ + requests（requirements.txt に既存）。google-auth 等は不要。

使い方:
  1. GCP で OAuth クライアント（種別: デスクトップアプリ）を作成し、
     client_id と client_secret を控える（docs/gmail_oauth_setup.md 参照）。
  2. このスクリプトを実行:
        GMAIL_OAUTH_CLIENT_ID=xxx GMAIL_OAUTH_CLIENT_SECRET=yyy \
            python scripts/get-gmail-refresh-token.py
     （環境変数の代わりに対話入力も可）
  3. 表示された URL をブラウザで開き、recruit@takagi.bz でログイン → 許可。
     ローカルサーバーが callback を受け取り refresh token を表示する。
  4. 出力された GMAIL_OAUTH_REFRESH_TOKEN を Railway に設定する。

注意:
- access_type=offline ＋ prompt=consent を必ず付ける（refresh token を確実に得るため）。
- OAuth 同意画面は "In production" に publish しておくこと。Testing のままだと
  refresh token が7日で失効する（[[feedback_google_auth_standard]]）。
"""
import http.server
import os
import sys
import threading
import urllib.parse
import webbrowser

import requests

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"
# デスクトップアプリ型クライアントで使うループバック redirect。
REDIRECT_HOST = "127.0.0.1"
REDIRECT_PORT = int(os.getenv("GMAIL_OAUTH_REDIRECT_PORT", "8765"))
REDIRECT_URI = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}/"

_auth_code_holder = {"code": None, "error": None}


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


def _prompt(name: str, env_key: str) -> str:
    val = os.getenv(env_key)
    if val:
        return val
    return input(f"{name} ({env_key}): ").strip()


def main() -> int:
    client_id = _prompt("OAuth Client ID", "GMAIL_OAUTH_CLIENT_ID")
    client_secret = _prompt("OAuth Client Secret", "GMAIL_OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("ERROR: client_id と client_secret は必須です。", file=sys.stderr)
        return 1

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
    }
    auth_url = AUTH_URI + "?" + urllib.parse.urlencode(auth_params)
    print("\n以下の URL をブラウザで開いて、recruit@takagi.bz でログイン→許可してください:\n")
    print(auth_url + "\n")
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

    # 認証コード → refresh token に交換
    resp = requests.post(
        TOKEN_URI,
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
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
    print("以下を Railway の Variables に設定してください:\n")
    print(f"GMAIL_OAUTH_CLIENT_ID={client_id}")
    print(f"GMAIL_OAUTH_CLIENT_SECRET={client_secret}")
    print(f"GMAIL_OAUTH_REFRESH_TOKEN={refresh_token}")
    print("\n（GMAIL_IMAP_USER は Gmail アドレスのまま据え置きで OK）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

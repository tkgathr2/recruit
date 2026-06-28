#!/usr/bin/env python3
"""Gmail OAuth2 refresh token をワンタイムで取得するスクリプト（PKCE + client_secret 対応）。

社長（または運用者）がブラウザで1回「許可」を押すだけで refresh token が取れる。
取得した refresh token を Railway 環境変数 GMAIL_OAUTH_REFRESH_TOKEN に入れれば
以後は恒久稼働する（access token は自動更新・refresh token は原則失効しない）。

★ client_secret について（重要）
  デスクトップアプリ型 OAuth クライアント（recruit-gmail-oauth3）は、
  token 交換に client_secret が必要（Google が "Optional" と記載しているが、
  このクライアントは実際に要求する）。PKCE（RFC 7636）は必要条件だが十分条件ではない。
  「PKCE のみ・client_secret 不要」は本クライアントでは動作しない（400 invalid_request）。

  供給経路（優先順）:
    1. 環境変数 GMAIL_OAUTH_CLIENT_SECRET
    2. 環境変数 GMAIL_OAUTH_CLIENT_JSON（GCPからダウンロードしたOAuthクライアントJSONのパス）
       → JSON の installed.client_secret / installed.client_id を読み込む
    3. コマンドライン引数（argv[1]）

  secret が取得できない場合はエラーを表示して終了する。

依存: 標準ライブラリ + requests（requirements.txt に既存）。google-auth 等は不要。

使い方:
  1. GCP の OAuthクライアント（recruit-gmail-oauth3）の JSON をダウンロードしておく。
  2. 環境変数または引数で client_secret を指定して実行:
       # JSON ファイルで供給（推奨）:
       GMAIL_OAUTH_CLIENT_JSON=/path/to/client_secret.json python scripts/get-gmail-refresh-token.py
       # 環境変数で直接:
       GMAIL_OAUTH_CLIENT_SECRET=xxx python scripts/get-gmail-refresh-token.py
       # client_id が JSON と違う場合のみ引数で override:
       python scripts/get-gmail-refresh-token.py <client_id> <client_secret>
  3. 表示された URL をブラウザで開き、監視対象の受信箱を持つアカウント（現本番 = atsuhiro@takagi.bz）でログイン → 許可。
     （recruit@takagi.bz は存在しないアドレスのため使用不可）
     ローカルサーバーが callback を受け取り refresh token を表示する。
  4. 出力された GMAIL_OAUTH_CLIENT_ID と GMAIL_OAUTH_REFRESH_TOKEN と
     GMAIL_OAUTH_CLIENT_SECRET を Railway に設定する。

注意:
- access_type=offline ＋ prompt=consent を必ず付ける（refresh token を確実に得るため）。
- OAuth 同意画面は "In production" に publish しておくこと。Testing のままだと
  refresh token が7日で失効する（[[feedback_google_auth_standard]]）。
- PKCE は常に使う（code_verifier を token 交換でも送る）。
"""
import base64
import hashlib
import http.server
import json
import os
import secrets
import sys
import threading
import urllib.parse
import webbrowser

import requests

# https://mail.google.com/ が必須。gmail.readonly では IMAP XOAUTH2 が AUTHENTICATIONFAILED で失敗する。
GMAIL_READONLY_SCOPE = "https://mail.google.com/"
AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"

# 既定のデスクトップアプリ型クライアント（「Claude Code MCP Desktop」= 本番稼働中の実値）。
# client_secret は必須（下記の供給経路を参照）。
# ※ recruit-gmail-oauth3 クライアント (235822259813-7jdk1qosim8dj1lvej712br6e2i5iuam...) は
#   代替（任意）。現本番では「Claude Code MCP Desktop」を使用。
DEFAULT_CLIENT_ID = "235822259813-c9851j36ke8n0ne2jnclai4irktjr76d.apps.googleusercontent.com"

# デスクトップアプリ型クライアントで使うループバック redirect。
REDIRECT_HOST = "127.0.0.1"
REDIRECT_PORT = int(os.getenv("GMAIL_OAUTH_REDIRECT_PORT", "8765"))
REDIRECT_URI = f"http://{REDIRECT_HOST}:{REDIRECT_PORT}/"

_auth_code_holder = {"code": None, "error": None}
_server_done = threading.Event()


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


def _resolve_client_credentials(argv: list) -> tuple:
    """client_id と client_secret を優先順で解決して返す。

    優先順:
      client_id:
        引数[0] > 環境変数 GMAIL_OAUTH_CLIENT_ID > JSONファイル > DEFAULT_CLIENT_ID
      client_secret:
        環境変数 GMAIL_OAUTH_CLIENT_SECRET > JSONファイル(GMAIL_OAUTH_CLIENT_JSON) > 引数[1]

    Returns:
        (client_id, client_secret) のタプル。client_secret が取得できない場合は None。
    """
    # --- client_secret の解決 ---
    client_secret = (os.getenv("GMAIL_OAUTH_CLIENT_SECRET") or "").strip() or None

    # JSON ファイルからの読み込み（環境変数 GMAIL_OAUTH_CLIENT_JSON）
    json_client_id = None
    json_client_secret = None
    client_json_path = (os.getenv("GMAIL_OAUTH_CLIENT_JSON") or "").strip()
    if client_json_path:
        try:
            with open(client_json_path, encoding="utf-8") as f:
                cj = json.load(f)
            # GCP からダウンロードした JSON は {"installed": {...}} or {"web": {...}}
            inner = cj.get("installed") or cj.get("web") or {}
            json_client_id = (inner.get("client_id") or "").strip() or None
            json_client_secret = (inner.get("client_secret") or "").strip() or None
        except Exception as e:
            print(f"WARNING: GMAIL_OAUTH_CLIENT_JSON の読み込みに失敗しました: {e}", file=sys.stderr)

    # 環境変数 > JSON > 引数 の順で client_secret を決める
    if not client_secret and json_client_secret:
        client_secret = json_client_secret
    if not client_secret and len(argv) >= 2:
        client_secret = argv[1].strip() or None

    # --- client_id の解決 ---
    client_id = (
        (argv[0].strip() if argv else "")
        or (os.getenv("GMAIL_OAUTH_CLIENT_ID") or "").strip()
        or json_client_id
        or DEFAULT_CLIENT_ID
    )

    return client_id, client_secret


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    """ループバック OAuth callback ハンドラー。

    favicon等のコードなしリクエストは無視して待機を継続し、
    ?code= を含むリクエストが来たときのみ交換処理へ進む。
    """

    def do_GET(self):  # noqa: N802 - http.server の規約
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        code = params.get("code", [None])[0]
        error = params.get("error", [None])[0]

        if not code and not error:
            # favicon 等の code なしリクエスト → 待機継続
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"waiting for OAuth callback...")
            return

        # code または error を受信 → ホルダーに記録してサーバー終了を通知
        _auth_code_holder["code"] = code
        _auth_code_holder["error"] = error

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if code:
            body = "<h2>認証に成功しました。このタブを閉じてターミナルに戻ってください。</h2>"
        else:
            body = f"<h2>認証に失敗しました: {error}</h2>"
        self.wfile.write(body.encode("utf-8"))

        # サーバーに終了シグナルを送る
        _server_done.set()

    def log_message(self, *args):  # サーバーログを抑制
        pass


def _serve_until_code(timeout: int = 300) -> None:
    """code を受け取るまでループでリクエストを処理する。

    favicon 等の中間リクエストでサーバーが死なないよう、
    _server_done イベントがセットされるまで handle_request を繰り返す。
    """
    server = http.server.HTTPServer((REDIRECT_HOST, REDIRECT_PORT), _CallbackHandler)
    deadline = __import__("time").time() + timeout
    try:
        while not _server_done.is_set():
            remaining = deadline - __import__("time").time()
            if remaining <= 0:
                break
            server.timeout = min(1.0, remaining)
            server.handle_request()
    finally:
        server.server_close()


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv

    client_id, client_secret = _resolve_client_credentials(argv)

    if not client_id:
        print("ERROR: client_id を決定できませんでした。", file=sys.stderr)
        return 1

    if not client_secret:
        print(
            "ERROR: client_secret が取得できませんでした。\n"
            "このデスクトップアプリ型クライアントは PKCE のみでは動作しません\n"
            "（Google が 400 invalid_request: client_secret is missing を返します）。\n"
            "\n"
            "以下のいずれかで client_secret を指定してください:\n"
            "  1. 環境変数 GMAIL_OAUTH_CLIENT_SECRET=<secret>\n"
            "  2. 環境変数 GMAIL_OAUTH_CLIENT_JSON=<GCPからDLしたJSONファイルのパス>\n"
            "     （JSON内の installed.client_secret が自動で読み込まれます）\n"
            "  3. 引数: python scripts/get-gmail-refresh-token.py <client_id> <client_secret>",
            file=sys.stderr,
        )
        return 1

    # PKCE ペアを生成（auth URL に code_challenge、token 交換で code_verifier を送る）。
    code_verifier, code_challenge = generate_pkce_pair()

    # ローカル callback サーバーをバックグラウンドスレッドで起動
    _server_done.clear()
    # code/error を初期化（テスト時は呼び出し元が事前セット可能なので、
    # 既にセットされていれば即座に _server_done をセットしてスキップ）
    if not _auth_code_holder.get("code") and not _auth_code_holder.get("error"):
        _auth_code_holder["code"] = None
        _auth_code_holder["error"] = None
    else:
        # テスト用: code が事前にセットされている場合はサーバー待機をスキップ
        _server_done.set()
    server_thread = threading.Thread(target=_serve_until_code, kwargs={"timeout": 300}, daemon=True)
    server_thread.start()

    auth_params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": GMAIL_READONLY_SCOPE,
        "access_type": "offline",      # refresh token を得るために必須
        "prompt": "consent",           # 毎回 refresh token を確実に返させる
        "code_challenge": code_challenge,    # PKCE（必須・client_secret と併用）
        "code_challenge_method": "S256",
    }
    auth_url = AUTH_URI + "?" + urllib.parse.urlencode(auth_params)
    print("\n以下の URL をブラウザで開いて、監視対象の受信箱を持つアカウント（現本番 = atsuhiro@takagi.bz）でログイン→許可してください:\n", flush=True)
    print(auth_url + "\n", flush=True)
    print(f"使用する client_id: {client_id}", flush=True)
    print("認証方式: PKCE + client_secret（両方必須）\n", flush=True)
    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    print(f"ローカルで callback を待機中... ({REDIRECT_URI})", flush=True)
    # _server_done がセットされるか 300 秒でタイムアウト
    _server_done.wait(timeout=305)
    server_thread.join(timeout=5)

    if _auth_code_holder["error"]:
        print(f"ERROR: 認証が拒否されました: {_auth_code_holder['error']}", file=sys.stderr)
        return 1
    code = _auth_code_holder["code"]
    if not code:
        print("ERROR: 認証コードを取得できませんでした（タイムアウト）。", file=sys.stderr)
        return 1

    # 認証コード → refresh token に交換（PKCE: code_verifier + client_secret 両方送る）。
    token_data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,  # 必須
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
        "code_verifier": code_verifier,   # PKCE も併用
    }

    resp = requests.post(TOKEN_URI, data=token_data, timeout=30)
    if resp.status_code >= 400:
        print(
            f"ERROR: token 交換に失敗しました (status={resp.status_code}): {resp.text}",
            file=sys.stderr,
        )
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
    print(f"GMAIL_OAUTH_CLIENT_SECRET={client_secret}")
    print("\n（GMAIL_IMAP_USER は Gmail アドレスのまま据え置きで OK）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

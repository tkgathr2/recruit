# Gmail OAuth2（refresh token）認証セットアップ手順

Indeed応募通知Bot（recruit）の Gmail 取得認証を、失効を繰り返すアプリパスワード(IMAP)から
**OAuth2 refresh token 方式**へ移行するための手順書。

## なぜOAuth2に変えるのか

- アプリパスワード(IMAP)は数週間で失効再発（2026/5/8・6/27）し、そのたびに人手での
  再発行＋Railway 環境変数更新が必要だった。
- OAuth2 の **refresh token は原則失効しない**（取消・パスワード変更・6か月未使用を除く）。
  access token はプログラムが自動で更新するため、再発行運用が不要になる。
- 社内標準も Google OAuth（[[feedback_google_auth_standard]]）。

## ★ client_secret は不要（PKCE 方式）

デスクトップアプリ型 OAuth クライアントは **PKCE（RFC 7636）** を使えば、
**client_secret 無しで** authorization code 交換・refresh ができる（gcloud / rclone 等の
CLI と同じ方式）。Google 公式ドキュメント "OAuth 2.0 for Mobile & Desktop Apps"
（https://developers.google.com/identity/protocols/oauth2/native-app ）でも、
installed app の token 交換・refresh で `client_secret` は **"Optional"**、PKCE は
明示的にサポート・推奨されている。

→ これにより GCP の **client_secret 取得・JSON ダウンロードが不要**になり、
  「secret の壁」を完全に回避できる。`GMAIL_OAUTH_CLIENT_SECRET` を設定した場合のみ
  併用する（任意）。

## 認証方式の自動切替（後方互換）

`src/main.py` は以下のように認証方式を自動選択する（移行期の安全網）:

| 条件 | 使う認証方式 |
|------|-------------|
| `GMAIL_OAUTH_CLIENT_ID` / `GMAIL_OAUTH_REFRESH_TOKEN` が設定済み（secret は任意） | **OAuth2 (IMAP XOAUTH2)** |
| 上記が未設定 | アプリパスワード IMAP（`GMAIL_IMAP_PASSWORD`） |

→ OAuth の2変数を入れた瞬間に OAuth へ切り替わる。問題があれば2変数を消せばアプリパスワードに戻る。

## 必要な環境変数（Railway）

| 環境変数名 | 用途 | 必須 |
|-----------|------|------|
| `GMAIL_IMAP_USER` | Gmail アドレス（IMAP ユーザー＝`recruit@takagi.bz`）。OAuth でも共用 | ○ |
| `GMAIL_OAUTH_CLIENT_ID` | GCP OAuth クライアント ID | OAuth時○ |
| `GMAIL_OAUTH_REFRESH_TOKEN` | 同意フローで取得した refresh token | OAuth時○ |
| `GMAIL_OAUTH_CLIENT_SECRET` | GCP OAuth クライアントシークレット（**PKCE 方式のため不要**・任意） | 任意 |
| `GMAIL_IMAP_PASSWORD` | アプリパスワード（OAuth未設定時のフォールバック） | OAuth時は不要 |

スコープは **`https://www.googleapis.com/auth/gmail.readonly`（読み取り専用・最小権限）** のみ。

---

## A. GCP の OAuth クライアント（既存の Desktop 型をそのまま使う）

PKCE 方式なので **client_secret は不要**。既定で使うのは既存の Desktop 型クライアント
`recruit-gmail-oauth3`:

```
client_id = 235822259813-7jdk1qosim8dj1lvej712br6e2i5iuam.apps.googleusercontent.com
```

スクリプトはこの client_id を既定値として持っているため、**GCP 側で新しく作業する必要はない**。
別クライアントを使う場合のみ、種別「デスクトップアプリ」で作成し（client_secret は控えなくてよい）、
その client_id を渡す。

前提（初回のみ確認）:
- **Gmail API** が有効化されていること。
- **OAuth 同意画面** が「本番（In production）」に publish 済みであること
  （Testing のままだと refresh token が7日で失効する。既知の落とし穴）。
- スコープに `.../auth/gmail.readonly` が含まれること。
- デスクトップアプリ型はループバック redirect `http://127.0.0.1:<port>/` を自動許可するため、
  redirect URI の手動登録は不要。

---

## B. refresh token をワンタイムで取得する（社長は1クリックだけ）

`scripts/get-gmail-refresh-token.py` がローカルで同意フローを回し、refresh token を表示する。
ブラウザで1回「許可」を押すだけで取得できる。

```bash
# リポジトリルートで（requests が入っていること: pip install -r requirements.txt）
# 既定の client_id を使うので引数も環境変数も不要。client_secret も不要（PKCE）。
python scripts/get-gmail-refresh-token.py

# 別の client_id を使う場合のみ:
#   python scripts/get-gmail-refresh-token.py <client_id>
```

1. 実行するとブラウザが開く（開かなければ表示された URL を手動で開く）。
2. **`recruit@takagi.bz` でログイン → 「許可」をクリック**（←社長の一手はここだけ）。
3. ローカルサーバーが callback を受け取り、ターミナルに以下を表示する:
   ```
   GMAIL_OAUTH_CLIENT_ID=...
   GMAIL_OAUTH_REFRESH_TOKEN=...
   # GMAIL_OAUTH_CLIENT_SECRET は PKCE 方式のため不要
   ```

> 「許可」を押す Google アカウントは、recruit が読みたい受信箱を持つアカウント
> （= `recruit@takagi.bz`）であること。別アカウントで許可すると、そのアカウントの
> メールを読むことになる。

---

## C. Railway に投入する（真田が実施）

取得した2値を recruit サービス（Railway `desirable-smile / recruit`）の Variables に設定する。

```bash
railway variables \
  --set "GMAIL_OAUTH_CLIENT_ID=<値>" \
  --set "GMAIL_OAUTH_REFRESH_TOKEN=<値>"
# client_secret は PKCE 方式のため設定不要
```

- `GMAIL_IMAP_USER`（=`recruit@takagi.bz`）は据え置き。
- 設定後にデプロイ/再起動 → 起動ログに `Gmail auth method: OAuth2 (XOAUTH2 refresh token)` が
  出ること、IMAP connect の `auth=XOAUTH2 ... success` が出ることを確認する。
- 動作確認後、フォールバック用の `GMAIL_IMAP_PASSWORD` は残しても消してもよい
  （OAuth が設定されていれば使われない）。

---

## D. 失効ゼロを保つ運用

- refresh token は原則失効しないが、次の場合に無効化される:
  - ユーザーが Google アカウントのアクセス権を取り消した
  - Google パスワード変更（場合により）
  - **6か月間1回も使われなかった**（recruit は60秒間隔でポーリングするので通常起こらない）
  - OAuth 同意画面が Testing のまま（→ 7日失効。必ず In production に）
- refresh token が失効した場合は、起動ログ／Slack に
  `Gmail OAuth の認証に失敗しました（refresh token が無効/失効＝要再同意）` が出る。
  その時は **B の手順を1回やり直して `GMAIL_OAUTH_REFRESH_TOKEN` を更新**するだけ。

---

## 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2026-06-27 | OAuth2 refresh token 方式へ移行（アプリパスワード失効根絶） |
| 2026-06-28 | PKCE 方式へ変更（client_secret 不要・既定 Desktop client を内蔵） |

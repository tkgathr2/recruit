# Indeed応募→Slack・LINE通知 Ver3 運用ガイド

## 1. 概要

### システム名
Indeed応募→Slack・LINE通知 Ver3 (Python + Railway)

### 目的
Indeed および ジモティー からの求人応募メールを自動的に取得し、Slack と LINE に通知する自動化システムです。

### システム構成
```
Gmail (IMAP)
    ↓
Python アプリケーション (Railway 常時稼働)
    ↓
Slack Webhook & LINE Messaging API
```

### 主な機能
- Gmail の IMAP 経由で未読メールをチェック
- Indeed / ジモティーからの応募メールを自動判別
- 応募者名と応募内容確認 URL を抽出
- Slack と LINE に自動通知
- test/prod モードによる環境切り替え機能

---

## 2. 必要な環境変数一覧

### 2-1. Gmail 関連（共通）

| 環境変数名 | 用途 | 例（ダミー値） | 必須 |
|-----------|------|---------------|------|
| `GMAIL_IMAP_HOST` | Gmail IMAP サーバー | `imap.gmail.com` | ○ |
| `GMAIL_IMAP_USER` | Gmail アカウント | `recruit@example.com` | ○ |
| `GMAIL_IMAP_PASSWORD` | Gmail アプリパスワード | `abcd efgh ijkl mnop` | ○ |

### 2-2. Slack 関連

| 環境変数名 | 用途 | 例（ダミー値） | 必須 |
|-----------|------|---------------|------|
| `SLACK_WEBHOOK_URL_TEST` | テスト用 Webhook URL | `https://hooks.slack.com/services/T.../B.../xxx` | テスト時のみ |
| `SLACK_WEBHOOK_URL_PROD` | 本番用 Webhook URL | `https://hooks.slack.com/services/T.../B.../yyy` | 本番時のみ |

### 2-3. LINE 関連

| 環境変数名 | 用途 | 例（ダミー値） | 必須 |
|-----------|------|---------------|------|
| `LINE_CHANNEL_ACCESS_TOKEN` | LINE Messaging API トークン | `eyJhbGciOiJIUzI1NiIsInR5cCI6...` | ○ |
| `LINE_TO_ID_TEST` | テスト送信先 ユーザー/グループ ID | `U1234567890abcdef` | テスト時のみ |
| `LINE_TO_ID_PROD` | 本番送信先 ユーザー/グループ ID | `C9876543210fedcba` | 本番時のみ |

### 2-4. MODE（環境切り替え）

| 環境変数名 | 用途 | 設定値 | 必須 |
|-----------|------|--------|------|
| `MODE` | テスト/本番の切り替え | `test` または `prod` | ○ |

- `MODE=test`: テスト環境（メッセージに「テストバージョン」プレフィックスが付く）
- `MODE=prod`: 本番環境（通常の通知メッセージ）
- 未設定の場合: デフォルトで `prod` として動作

### 2-5. その他（オプション）

| 環境変数名 | 用途 | 例（ダミー値） | 必須 |
|-----------|------|---------------|------|
| `LOG_DIR` | ログファイル保存先 | `/tmp` | × |

---

## 3. 本番切り替え手順（n8n → Ver3）

### 3-1. 事前準備

1. **Railway の Variables 設定**
   - Railway の管理画面で、本番用の環境変数を設定
   - 特に重要な項目:
     ```
     MODE=prod
     SLACK_WEBHOOK_URL_PROD=<本番用WebhookURL>
     LINE_TO_ID_PROD=<本番用ユーザーID>
     GMAIL_IMAP_USER=<Gmailアドレス>
     GMAIL_IMAP_PASSWORD=<Gmailアプリパスワード>
     LINE_CHANNEL_ACCESS_TOKEN=<LINEアクセストークン>
     ```

2. **環境変数の確認**
   - Railway のログで起動時の環境変数読み込み状況を確認
   - `WARNING: ... is not set` が出ていないことを確認

### 3-2. 並走期間による動作確認

**重要**: 移行確認のため、一時的に n8n と Ver3 を並走させます。この期間中は通知が二重になりますが、確認のために許容します。

1. **Ver3 を起動**
   - Railway で `MODE=prod` に設定して Deploy/Restart
   - ログで正常起動を確認

2. **並走テスト**
   - 実際の応募を1件待つ、または既存の未読応募メールで確認
   - Slack と LINE の両方で以下を確認:
     - n8n からの通知（従来フォーマット）
     - Ver3 からの通知（新フォーマット）
   - 両方が正しく届いていることを確認

3. **動作確認のチェックポイント**
   - Ver3 からの通知に「テストバージョン」プレフィックスが付いていないこと
   - 応募者名が正しく抽出されていること
   - 応募内容確認 URL が正しく含まれていること
   - Slack と LINE の両方に届いていること

### 3-3. n8n の停止

動作確認が完了したら、n8n 側を停止します。

1. **n8n 管理画面にアクセス**
   - 対象ワークフロー: 「Indeed応募→Slack&LINE通知」関連

2. **ワークフローを Disable に設定**
   - ワークフローの Active スイッチを OFF にする
   - または該当のトリガーを無効化

3. **最終確認**
   - 次の応募が来た際に、Ver3 からのみ通知が来ることを確認
   - n8n からの通知が来ないことを確認

### 3-4. 本番運用開始

- 以降は Ver3 のみが本番として稼働します
- 問題が発生した場合は、すぐに n8n を再度 Active にすることでロールバック可能

---

## 4. 日常運用のチェックポイント

### 4-1. 通知の確認先

**Slack**
- チャンネル: （設定した Webhook の送信先チャンネル）
- 通知フォーマット例:
  ```
  【Indeed応募】山田太郎 さんから応募がありました。

  応募内容はこちら:
  https://indeed.com/applications/...
  ```

**LINE**
- 送信先: （設定した TO_ID のユーザーまたはグループ）
- 通知フォーマット例:
  ```
  山田太郎 さんからIndeedに応募がありました。

  詳細はこちら:
  https://indeed.com/applications/...
  ```

### 4-2. 日常チェック項目

1. **応募通知の到着確認**
   - 応募があった際に、Slack と LINE の両方に通知が届いているか
   - 通知内容（応募者名、URL）が正しいか

2. **一日あたりの応募数**
   - 通常の応募ペースと比べて極端に少ない/多い場合は要確認
   - 目安: 過去の平均応募数と大きく乖離していないか

3. **Railway の稼働状況**
   - Railway の管理画面で、アプリケーションが正常稼働しているか確認
   - エラーログが大量に出ていないか

### 4-3. 定期確認（週次推奨）

- Railway のログを確認し、以下のようなエラーが出ていないか:
  - `ERROR: ...`
  - `WARNING: ... is not set`
  - IMAP 接続エラー
  - Slack/LINE API エラー

---

## 5. トラブルシュート手順（簡易版）

### 5-1. Slack にも LINE にも通知が来ない場合

**確認手順**:

1. **Railway のログを確認**
   - Railway 管理画面 > Deployments > Logs
   - エラーメッセージの内容を確認

2. **想定される原因と対処**:

   | エラー内容 | 考えられる原因 | 対処方法 |
   |-----------|--------------|---------|
   | Gmail 接続エラー | IMAP 設定が間違っている | `GMAIL_IMAP_USER`, `GMAIL_IMAP_PASSWORD` を確認 |
   | `No Slack Webhook URL` | Slack 環境変数未設定 | `SLACK_WEBHOOK_URL_PROD` を設定 |
   | `LINE Token or TO ID missing` | LINE 環境変数未設定 | `LINE_CHANNEL_ACCESS_TOKEN`, `LINE_TO_ID_PROD` を設定 |
   | `UNSEEN: 0` | 未読メールがない | Gmail で未読応募メールがあるか確認 |

### 5-2. Slack は来るが LINE だけ来ない場合

**確認手順**:

1. **LINE 関連の環境変数を確認**
   - `LINE_CHANNEL_ACCESS_TOKEN` が正しく設定されているか
   - `LINE_TO_ID_PROD` が正しい送信先 ID か

2. **LINE Messaging API の設定を確認**
   - LINE Developers コンソールでチャンネルが有効か
   - アクセストークンの有効期限が切れていないか
   - Bot がグループに追加されているか（グループ送信の場合）

3. **Railway のログで LINE API のエラーを確認**
   - `400 Bad Request` → 送信先 ID が間違っている
   - `401 Unauthorized` → アクセストークンが無効
   - `403 Forbidden` → Bot がブロックされている可能性

### 5-3. 特定の応募だけ通知されていない場合

**確認手順**:

1. **Gmail のメールを確認**
   - 該当の応募メールが実際に届いているか
   - 未読（UNSEEN）状態になっているか
   - 迷惑メールフォルダに入っていないか

2. **件名をチェック**
   - Indeed: 「新しい応募者のお知らせ」が件名に含まれているか
   - ジモティー: 「ジモティー」が件名に含まれているか

3. **既読になっているか確認**
   - Ver3 で処理済みのメールは自動的に既読になります
   - すでに既読の場合は、再度通知されません

### 5-4. 緊急時のロールバック手順

問題が解決できない場合、一時的に n8n にロールバックできます。

1. **n8n のワークフローを Active にする**
   - n8n 管理画面で該当ワークフローの Active スイッチを ON

2. **Ver3 を一時停止（オプション）**
   - Railway で一時的に停止するか、`MODE=test` に切り替え

3. **問題を調査・修正後、再度 Ver3 に切り替え**

---

## 6. Railway での環境変数設定例

Railway 管理画面での設定例（本番環境）:

```
MODE=prod
GMAIL_IMAP_HOST=imap.gmail.com
GMAIL_IMAP_USER=recruit@takagi.bz
GMAIL_IMAP_PASSWORD=<16文字のアプリパスワード>
SLACK_WEBHOOK_URL_PROD=https://hooks.slack.com/services/T.../B.../xxx
LINE_CHANNEL_ACCESS_TOKEN=<LINEアクセストークン>
LINE_TO_ID_PROD=C9876543210fedcba
LOG_DIR=/tmp
```

---

## 7. 関連リンク

- GitHub リポジトリ: https://github.com/tkgathr2/recruit
- Railway プロジェクト: （Railway 管理画面のURL）
- LINE Developers コンソール: https://developers.line.biz/console/

---

## 8. 更新履歴

| 日付 | 変更内容 |
|------|---------|
| 2025-12-08 | 初版作成（Ver3 本番運用ガイド） |

---

## 9. 問い合わせ先

システムに関する質問や問題が発生した場合:
- 開発者: 高木産業株式会社
- Email: atsuhiro@takagi.bz

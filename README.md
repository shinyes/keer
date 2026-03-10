# Keer

Keer 是一个使用 Go 开发的轻量级后端服务，提供用户、Memo、附件、群组与消息能力。默认使用 SQLite，支持本地文件存储和 S3 对象存储。

## 功能概览

- 用户注册、登录、鉴权（Bearer Token）
- Memo 的增删改查、变更同步、标签统计
- 附件管理（Base64 直传、分片/断点续传、下载、缩略图）
- 群组、群消息与群标签管理
- 过滤表达式（CEL）与标签层级匹配
- 运行时管理控制台（用户、Token、注册开关、存储配置）

## 零知识原则（面向端到端加密）

- 后端不解析 Memo 明文内容，也不基于内容做检索或过滤
- 过滤仅面向可公开元数据：`creator_id`、`visibility`、`state`、`pinned`、`tags`
- `content` / `property` 相关过滤表达式会被拒绝
- 标签由客户端计算并上传，服务端只做持久化与匹配

## 环境要求

- Go `1.25.5`（或兼容 `1.25+`）
- Windows / Linux / macOS

## 快速启动

```powershell
$env:APP_ADDR=":12843"
$env:BASE_URL="http://localhost:12843"
$env:DB_PATH="./data/keer.db"
$env:UPLOADS_DIR="./data/uploads"
$env:KEER_API_VERSION="0.1"

# 可选：首次启动时自动创建引导用户与令牌
$env:BOOTSTRAP_USER="demo"
$env:BOOTSTRAP_TOKEN="demo-token"

go run ./cmd/server
```

服务启动后会进入 `keer>` 运行时控制台（和 HTTP 服务同进程）。
输入 `help` 查看命令，输入 `exit` 仅退出控制台，不会停止 HTTP 服务。

## 配置项

- `APP_ADDR`：监听地址，默认 `:12843`
- `BASE_URL`：服务基地址，默认 `http://localhost:12843`
- `DB_PATH`：SQLite 文件路径，默认 `./data/keer.db`
- `UPLOADS_DIR`：本地附件目录（仅 local 存储时生效），默认 `./data/uploads`
- `HTTP_BODY_LIMIT_MB`：HTTP 请求体上限（MiB），默认 `64`
- `KEER_API_VERSION`：`/api/v1/instance/profile` 返回字段 `keer_api_version`，默认 `0.1`
- `ALLOW_REGISTRATION`：是否允许公开注册，默认 `true`
- `BOOTSTRAP_USER`：引导用户名，默认 `demo`
- `BOOTSTRAP_TOKEN`：引导 token，默认空（为空则不创建引导 token）

## 存储模式

默认存储后端为 `local`。可在运行时控制台切换到 `s3`，配置会持久化到数据库 `system_settings`。

```text
storage status
storage set-local
storage wizard
storage set-s3 --endpoint "https://<endpoint>" --region "auto" --bucket "<bucket>" --access-key-id "<ak>" --access-key-secret "<sk>" --use-path-style=true
```

切换存储后端后需要重启服务生效。

## 运行时控制台命令

```text
user create <username> <password> [display_name] [role]
token create <username_or_id> [description] [--ttl 7d|24h]
token list <username_or_id> [--all]
token revoke <token_id>
registration status
registration enable
registration disable
storage status
storage set-local
storage set-s3 ...
storage wizard
help
exit
```

说明：

- `token create` 默认 `--ttl 7d`，支持 `d/day/days` 与 `Go duration`（如 `30d`、`24h`、`30m`）
- `registration enable/disable` 会立即影响 `POST /api/v1/users`

## 鉴权与登录

登录接口：

```http
POST /api/v1/auth/signin
Content-Type: application/json

{
  "passwordCredentials": {
    "username": "alice",
    "password": "alice-password"
  }
}
```

成功后返回 `accessToken`，后续请求使用：

```http
Authorization: Bearer <accessToken>
```

## API 概览

公开接口：

- `GET /api/v1/instance/profile`
- `POST /api/v1/auth/signin`
- `POST /api/v1/users`

用户接口：

- `GET /api/v1/auth/me`
- `GET /api/v1/users/{name}`
- `PATCH /api/v1/users/{name}`
- `GET /api/v1/users/{name}/settings/GENERAL`
- `GET /api/v1/users/{name}:getStats`
- `GET /api/v1/users/batch`
- `GET /api/v1/users/changes`

Memo 接口：

- `GET /api/v1/memos`
- `GET /api/v1/memos/changes`
- `POST /api/v1/memos`
- `PATCH /api/v1/memos/{id}`
- `DELETE /api/v1/memos/{id}`

附件接口：

- `GET /api/v1/attachments`
- `POST /api/v1/attachments`
- `POST /api/v1/attachments/uploads`
- `HEAD /api/v1/attachments/uploads/{id}`
- `GET /api/v1/attachments/uploads/{id}/parts/{partNumber}`
- `PATCH /api/v1/attachments/uploads/{id}`
- `POST /api/v1/attachments/uploads/{id}/complete`
- `DELETE /api/v1/attachments/uploads/{id}`
- `DELETE /api/v1/attachments/{id}`
- `GET /file/attachments/{id}/{filename}`
- `GET /file/attachments/{id}/thumbnail/{filename}`
- `GET /file/avatars/{id}`

群组接口：

- `GET /api/v1/groups`
- `POST /api/v1/groups`
- `POST /api/v1/groups/{id}/join`
- `PATCH /api/v1/groups/{id}`
- `DELETE /api/v1/groups/{id}`
- `GET /api/v1/groups/{id}/messages`
- `POST /api/v1/groups/{id}/messages`
- `PATCH /api/v1/groups/{id}/messages/{messageId}`
- `DELETE /api/v1/groups/{id}/messages/{messageId}`
- `GET /api/v1/groups/{id}/tags`
- `POST /api/v1/groups/{id}/tags`

## 过滤表达式（CEL）

`GET /api/v1/memos` 与 `GET /api/v1/memos/changes` 的 `filter` 参数支持 CEL。

常见写法示例：

- `creator_id == 1 && visibility in ["PRIVATE"]`
- `tag in ["book"]`（匹配 `book` 和 `book/...`）
- `"work" in tags`
- `tags.exists(t, t.startsWith("book"))`

说明：

- 会先做一层 SQL 安全下推，再进行 CEL 最终求值
- 为保证性能与边界清晰，基于 `content` / `property` 的过滤会被拒绝

## 测试

```powershell
go test ./...
go vet ./...
```

## Docker Image Release

- Backend Docker image is built from [`Dockerfile`](./Dockerfile).
- GitHub Actions workflow: `.github/workflows/release-backend-docker.yml`
- Publish target: `ghcr.io/<owner>/<repo>:<version>` and `:latest` for stable versions.
- Trigger mode (push tag):
  - `1.2.3`
  - `1.2.3-alpha.1`
  - `1.2.3-beta.1`

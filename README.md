# Keer Backend

Keer 是一个使用 Go 开发的轻量级后端服务，提供用户、Memo、附件、群组与消息能力。默认使用 SQLite，支持本地文件存储和 S3 对象存储。

## 现在的配置原则

- 服务启动只依赖环境变量，不再从数据库读取 S3 配置
- 旧的 `storage_backend` / `storage_s3_*` 数据库设置在启动时会被清理
- `local` 与 `s3` 两种存储模式都通过环境变量切换

## 功能概览

- 用户注册、登录、鉴权（Bearer Token）
- Memo 的增删改查与变更同步
- 附件管理（Base64 直传、分片/断点续传、下载、缩略图）
- 群组、群消息与群标签管理
- 运行时管理控制台（用户、Token、注册开关）

## 零知识原则（面向端到端加密）

- 后端不解析 Memo 明文内容，也不基于内容做检索或过滤
- 过滤仅面向可公开元数据：`creator_id`、`visibility`、`state`、`pinned`、`tags`
- `content` / `property` 相关过滤表达式会被拒绝
- 标签由客户端计算并上传，服务端只做持久化与匹配

## 环境要求

- Go `1.25.5` 或兼容版本
- Windows / Linux / macOS
- Docker（如果使用容器部署）

## 5 分钟启动

### 1. 本地文件存储

PowerShell:

```powershell
$env:APP_ADDR=":12843"
$env:BASE_URL="http://localhost:12843"
$env:DB_PATH="./data/keer.db"
$env:UPLOADS_DIR="./data/uploads"
$env:STORAGE_BACKEND="local"
$env:JWT_SECRET="replace-with-a-long-random-secret"
$env:BOOTSTRAP_USER="demo"
$env:BOOTSTRAP_TOKEN="demo-token"

go run ./cmd/server
```

Bash:

```bash
export APP_ADDR=:12843
export BASE_URL=http://localhost:12843
export DB_PATH=./data/keer.db
export UPLOADS_DIR=./data/uploads
export STORAGE_BACKEND=local
export JWT_SECRET=replace-with-a-long-random-secret
export BOOTSTRAP_USER=demo
export BOOTSTRAP_TOKEN=demo-token

go run ./cmd/server
```

### 2. S3 对象存储

```powershell
$env:APP_ADDR=":12843"
$env:BASE_URL="https://api.example.com"
$env:DB_PATH="./data/keer.db"
$env:STORAGE_BACKEND="s3"
$env:S3_ENDPOINT="https://<endpoint>"
$env:S3_REGION="auto"
$env:S3_BUCKET="<bucket>"
$env:S3_ACCESS_KEY_ID="<access-key-id>"
$env:S3_ACCESS_KEY_SECRET="<access-key-secret>"
$env:S3_USE_PATH_STYLE="true"
$env:JWT_SECRET="replace-with-a-long-random-secret"

go run ./cmd/server
```

服务启动后会进入 `keer>` 运行时控制台。输入 `help` 查看命令，输入 `exit` 只退出控制台，不会停止 HTTP 服务。

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_ADDR` | `:12843` | HTTP 监听地址 |
| `BASE_URL` | `http://localhost:12843` | 服务基地址 |
| `DB_PATH` | `./data/keer.db` | SQLite 数据库路径 |
| `UPLOADS_DIR` | `./data/uploads` | 本地文件存储目录，仅 `local` 模式生效 |
| `HTTP_BODY_LIMIT_MB` | `64` | HTTP 请求体上限（MiB） |
| `KEER_API_VERSION` | `0.1` | `/api/v1/instance/profile` 返回的 API 版本 |
| `ALLOW_REGISTRATION` | `true` | 是否允许公开注册 |
| `BOOTSTRAP_USER` | `demo` | 首次启动时引导用户名 |
| `BOOTSTRAP_TOKEN` | 空 | 首次启动时引导 Personal Access Token；为空则不创建 |
| `JWT_SECRET` | `change-me-in-production` | JWT 签名密钥，生产环境必须覆盖 |
| `ACCESS_TOKEN_TTL` | `15m` | Access Token 有效期 |
| `REFRESH_TOKEN_TTL` | `720h` | Refresh Token 有效期 |
| `STORAGE_BACKEND` | `local` | 存储后端：`local` 或 `s3` |
| `S3_ENDPOINT` | 空 | S3 Endpoint，`s3` 模式必填 |
| `S3_REGION` | 空 | S3 Region，`s3` 模式必填 |
| `S3_BUCKET` | 空 | S3 Bucket，`s3` 模式必填 |
| `S3_ACCESS_KEY_ID` | 空 | S3 Access Key ID，`s3` 模式必填 |
| `S3_ACCESS_KEY_SECRET` | 空 | S3 Access Key Secret，`s3` 模式必填 |
| `S3_USE_PATH_STYLE` | `true` | 是否启用 path-style 访问 |

说明：

- 当 `STORAGE_BACKEND=s3` 时，所有 `S3_*` 必填项都会在启动时校验
- `ALLOW_REGISTRATION` 仍可被运行时控制台中的 `registration enable/disable` 持久化覆盖

## Docker

### 本地构建镜像

```bash
docker build -t keer-backend:local .
```

### 运行本地文件存储模式

```bash
docker run --rm -it \
  -p 12843:12843 \
  -e BASE_URL=http://localhost:12843 \
  -e STORAGE_BACKEND=local \
  -e JWT_SECRET=replace-with-a-long-random-secret \
  -e BOOTSTRAP_USER=demo \
  -e BOOTSTRAP_TOKEN=demo-token \
  -v keer-data:/data \
  keer-backend:local
```

### 运行 S3 模式

```bash
docker run --rm -it \
  -p 12843:12843 \
  -e BASE_URL=https://api.example.com \
  -e STORAGE_BACKEND=s3 \
  -e S3_ENDPOINT=https://<endpoint> \
  -e S3_REGION=auto \
  -e S3_BUCKET=<bucket> \
  -e S3_ACCESS_KEY_ID=<access-key-id> \
  -e S3_ACCESS_KEY_SECRET=<access-key-secret> \
  -e S3_USE_PATH_STYLE=true \
  -e JWT_SECRET=replace-with-a-long-random-secret \
  -v keer-data:/data \
  keer-backend:local
```

镜像默认：

- 暴露端口 `12843`
- 将数据库与本地上传目录落到 `/data`
- 默认环境变量：
  - `DB_PATH=/data/keer.db`
  - `UPLOADS_DIR=/data/uploads`

## 运行时控制台命令

```text
user create <username> <password> [display_name] [role]
token create <username_or_id> [description] [--ttl 7d|24h]
token list <username_or_id> [--all]
token revoke <token_id>
registration status
registration enable
registration disable
help
exit
```

说明：

- `token create` 默认 `--ttl 7d`
- `registration enable/disable` 会立即影响 `POST /api/v1/users`
- 存储后端不再支持运行时控制台修改，必须通过环境变量设置并重启服务

## GitHub Actions

仓库内已提供两条工作流：

### `release-binaries.yml`

- 触发条件：
  - `workflow_dispatch`
  - 推送标签 `v*`
- 产物：
  - Linux `amd64` / `arm64`
  - macOS `amd64` / `arm64`
  - Windows `amd64`
- 标签发布时会自动创建 GitHub Release 并上传压缩包

### `docker-publish.yml`

- 触发条件：
  - `workflow_dispatch`
  - 推送标签 `v*`
- 行为：
  - 使用仓库根目录 `Dockerfile`
  - 构建 `linux/amd64` 与 `linux/arm64`
  - 推送到 `ghcr.io/<owner>/keer-backend`
  - 发布 tag 推送会额外生成版本镜像标签：
    - 稳定版：`:v3.0.0`、`:3.0.0`、`:3.0`、`:3`
    - 预发布：`:v3.0.0-beta.1`、`:3.0.0-beta.1`

如果你准备直接启用镜像发布，需要确保仓库对 GitHub Container Registry 有 `packages: write` 权限。

## 与 Android 同版本发布

后端现在和 Android 使用同一套 release tag 规则：

- `v3.0.0`
- `v3.0.0-alpha.1`
- `v3.0.0-beta.1`

推荐发布顺序：

1. Android 更新 `versionName/versionCode`
2. Backend 确认当前提交可发布
3. 分别在两个仓库推送同一个 tag

效果：

- Android 会发布 APK 到 GitHub Release
- Backend 会发布多平台二进制到 GitHub Release
- Backend 会同步推送 Docker 镜像到 `ghcr.io/<owner>/keer-backend`
- 当 tag 为 `v3.0.0` 时，Docker 镜像至少会带上 `:v3.0.0` 和 `:3.0.0`
- Backend 不再发布 `:main` 或 `:latest`

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

成功后返回 `accessToken` 与 `refreshToken`，后续请求使用：

```http
Authorization: Bearer <accessToken>
```

## API 概览

公开接口：

- `GET /api/v1/instance/profile`
- `POST /api/v1/auth/signin`
- `POST /api/v1/auth/refresh`
- `POST /api/v1/users`

用户接口：

- `GET /api/v1/auth/me`
- `GET /api/v1/users/{name}`
- `PATCH /api/v1/users/{name}`
- `GET /api/v1/users/{name}/settings/GENERAL`
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

## 测试

```bash
go test ./...
go vet ./...
```

## Docker Image Release

- Backend Docker image is built from [`Dockerfile`](./Dockerfile).
- GitHub Actions workflow: `.github/workflows/docker-publish.yml`
- Publish target: `ghcr.io/<owner>/keer-backend`
- Stable tag release publishes `:vX.Y.Z`, `:X.Y.Z`, `:X.Y`, and `:X`
- Prerelease tag release publishes `:vX.Y.Z-beta.N` or `:vX.Y.Z-alpha.N`, plus the stripped `:X.Y.Z-beta.N` / `:X.Y.Z-alpha.N`
- Trigger mode (push tag):
  - `v3.0.0`
  - `v3.0.0-alpha.1`
  - `v3.0.0-beta.1`

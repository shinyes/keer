# Keer Backend

Keer 是一个使用 Go 开发的轻量级后端服务，提供用户、Memo、附件、群组与消息能力。默认使用 SQLite，支持本地文件存储和 S3 对象存储。

## 现在的配置原则

- 服务启动只依赖环境变量，不再从数据库读取 S3 配置
- `local` 与 `s3` 两种存储模式都通过环境变量切换

## 功能概览

- 用户注册、登录、鉴权（Bearer Token）
- Memo 的增删改查与变更同步
- 附件管理（Base64 直传、分片/断点续传、下载、缩略图）
- 群组、群消息与群标签管理
- 管理员接口（当前包含存储清理）

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
$env:DB_PATH="./data/keer.db"
$env:UPLOADS_DIR="./data/uploads"
$env:STORAGE_BACKEND="local"
$env:JWT_SECRET="replace-with-a-long-random-secret"

go run ./cmd/server
```

Bash:

```bash
export APP_ADDR=:12843
export DB_PATH=./data/keer.db
export UPLOADS_DIR=./data/uploads
export STORAGE_BACKEND=local
export JWT_SECRET=replace-with-a-long-random-secret

go run ./cmd/server
```

### 2. S3 对象存储

```powershell
$env:APP_ADDR=":12843"
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

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_ADDR` | `:12843` | HTTP 监听地址 |
| `DB_PATH` | `./data/keer.db` | SQLite 数据库路径 |
| `UPLOADS_DIR` | `./data/uploads` | 本地文件存储目录，仅 `local` 模式生效 |
| `HTTP_BODY_LIMIT_MB` | `64` | HTTP 请求体上限（MiB） |
| `ALLOW_REGISTRATION` | `true` | 是否允许公开注册 |
| `ADMIN_USERS` | 空 | 逗号分隔的管理员用户名列表，服务启动时会把这些用户提升为 `ADMIN` |
| `JWT_SECRET` | 必填 | JWT 签名密钥，不能为空，也不能使用 `change-me-in-production` |
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
- `ADMIN_USERS` 使用用户名匹配，例如 `ADMIN_USERS=alice,bob`
- 当前默认不配置 CORS；如果未来需要浏览器跨域访问，再单独补相关能力

## Docker

### 本地构建镜像

```bash
docker build -t keer:local .
```

### 运行本地文件存储模式

```bash
docker run --rm -it \
  -p 12843:12843 \
  -e STORAGE_BACKEND=local \
  -e ADMIN_USERS=alice \
  -e JWT_SECRET=replace-with-a-long-random-secret \
  -v keer-data:/data \
  keer:local
```

### 运行 S3 模式

```bash
docker run --rm -it \
  -p 12843:12843 \
  -e STORAGE_BACKEND=s3 \
  -e ADMIN_USERS=alice \
  -e S3_ENDPOINT=https://<endpoint> \
  -e S3_REGION=auto \
  -e S3_BUCKET=<bucket> \
  -e S3_ACCESS_KEY_ID=<access-key-id> \
  -e S3_ACCESS_KEY_SECRET=<access-key-secret> \
  -e S3_USE_PATH_STYLE=true \
  -e JWT_SECRET=replace-with-a-long-random-secret \
  -v keer-data:/data \
  keer:local
```

### 使用 Docker Compose 部署

可以在项目目录旁创建 `compose.yaml`：

```yaml
services:
  keer:
    image: ghcr.io/shinyes/keer:v3.1.0
    container_name: keer
    restart: unless-stopped
    ports:
      - "12843:12843"
    environment:
      STORAGE_BACKEND: local
      ADMIN_USERS: alice
      JWT_SECRET: replace-with-a-long-random-secret
    volumes:
      - ./keer-data:/data

volumes:
  keer-data:
```

启动：

```bash
docker compose up -d
```

查看日志：

```bash
docker compose logs -f keer
```

停止并删除容器：

```bash
docker compose down
```

如果你要改成 S3 模式，只需要把 `STORAGE_BACKEND` 改为 `s3`，并补上 `S3_ENDPOINT`、`S3_REGION`、`S3_BUCKET`、`S3_ACCESS_KEY_ID`、`S3_ACCESS_KEY_SECRET`、`S3_USE_PATH_STYLE` 这些环境变量。

## 管理员配置

当前管理员有两种来源：

- `HOST` 角色用户
- 通过 `ADMIN_USERS` 环境变量在启动时提升为 `ADMIN` 的用户

推荐做法：

1. 先正常注册一个账号，例如 `alice`
2. 重启服务时设置 `ADMIN_USERS=alice`
3. 该账号重新登录后，就会在 Android 设置页看到“管理员”分组

当前管理员功能：

- 清除孤儿文件

说明：

- 清除孤儿文件会扫描所有已配置的存储后端，不只扫描默认存储
- 因此同时支持：
  - 纯本地文件存储
  - 纯 S3
  - 本地 + S3 混合存储
- 当前接口路径为 `POST /api/v1/admin/storage/cleanup-orphans`

镜像默认：

- 暴露端口 `12843`
- 将数据库与本地上传目录落到 `/data`
- 默认环境变量：
  - `DB_PATH=/data/keer.db`
  - `UPLOADS_DIR=/data/uploads`
- 运行镜像时必须保证 `/data` 可写

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
  - 推送到 `ghcr.io/<owner>/keer`
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
- Backend 会同步推送 Docker 镜像到 `ghcr.io/<owner>/keer`
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

- `POST /api/v1/auth/signin`
- `POST /api/v1/auth/refresh`
- `POST /api/v1/users`

用户接口：

- `GET /api/v1/auth/me`
- `GET /api/v1/users/{name}`
- `PATCH /api/v1/users/{name}`
- `GET /api/v1/users/{name}/settings/GENERAL`
- `GET /api/v1/users/batch`
- `GET /api/v1/users/changes`（保留）

同步接口：

- `POST /api/v1/sync/pull`
  - 请求体：`cursor`、`domains`、`groupScopes`、`limit`
  - 响应体：`nextCursor`、`hasMore`、`patches{memos,users,groups,groupMessages,settings}`
  - 同步域：`MEMOS` / `USERS` / `GROUPS` / `GROUP_MESSAGES` / `SETTINGS`

Memo 接口：

- `GET /api/v1/memos`
- `GET /api/v1/memos/changes`（保留）
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
- Publish target: `ghcr.io/<owner>/keer`
- Stable tag release publishes `:vX.Y.Z`, `:X.Y.Z`, `:X.Y`, and `:X`
- Prerelease tag release publishes `:vX.Y.Z-beta.N` or `:vX.Y.Z-alpha.N`, plus the stripped `:X.Y.Z-beta.N` / `:X.Y.Z-alpha.N`
- Trigger mode (push tag):
  - `v3.0.0`
  - `v3.0.0-alpha.1`
  - `v3.0.0-beta.1`

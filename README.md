# Keer

GitHub: https://github.com/shinyes/keer

这是一个面向 `MoeMemosAndroid`（v1 风格 API）的轻量后端，核心目标是对齐 `usememos/memos` 的标签行为，并支持 S3 附件存储。

当前已实现：

- Markdown 标签解析（`goldmark + 自定义 inline parser`）
- 服务端持久化 memo payload（`payload_json`，含 `tags` 与 `property`）
- `getStats.tagCount` 基于 payload tags 统计
- `tag in ["..."]` 层级匹配（`book` 命中 `book` 和 `book/...`）
- 完整 CEL 过滤表达式（含组合逻辑与宏）
- `tags.exists(...)`
- `"x" in tags`
- 附件本地存储与 S3 兼容存储

当前未实现：

- 与上游 `usememos/memos` 完全一致的 SQL 过滤渲染器（当前实现为“安全子集下推 + CEL 最终求值”）

## 已实现范围

- API：覆盖 `MoeMemosAndroid` v1 联调所需关键接口
- 存储：SQLite + 本地文件系统 / S3（二选一）
- 鉴权：Bearer Token（个人访问令牌）
- 标签：服务端解析与输出，不信任客户端 tags
- 过滤：CEL 编译执行（含 tags.exists / `"x" in tags`）
- 查询优化：CEL 条件安全下推到 SQL（命中已识别子表达式）
- 运维：提供 payload 回填 CLI

## 环境要求

- Go `1.25+`
- Windows / Linux / macOS
- 首次构建需要联网下载 Go 依赖

## 快速启动（本地存储）

PowerShell 示例：

```powershell
$env:APP_ADDR=":8080"
$env:BASE_URL="http://localhost:8080"
$env:DB_PATH="./data/keer.db"
$env:UPLOADS_DIR="./data/uploads"
$env:MEMOS_VERSION="0.26.1"

# 首次联调用的引导账号与令牌
$env:BOOTSTRAP_USER="demo"
$env:BOOTSTRAP_TOKEN="demo-token"

go run ./cmd/server
```

Android 端将服务地址配置为：`http://<你的主机>:8080/`，Token 配置为 `demo-token`。

## 快速配置（S3 存储）

```powershell
# 存储配置持久化在数据库，不再使用 STORAGE_BACKEND/S3_* 环境变量
go run ./cmd/server
# 进入 keer> 控制台后执行：
storage wizard

# 或使用参数方式：
storage set-s3 `
  --endpoint "https://<你的S3地址>" `
  --region "auto" `
  --bucket "memos" `
  --access-key-id "<access-key-id>" `
  --access-key-secret "<access-key-secret>" `
  --use-path-style=true

# 切换后需重启服务生效
```

## 环境变量说明

- `APP_ADDR`：监听地址，默认 `:8080`
- `BASE_URL`：服务基地址，默认 `http://localhost:8080`
- `DB_PATH`：SQLite 文件路径，默认 `./data/keer.db`
- `UPLOADS_DIR`：本地附件目录，默认 `./data/uploads`（仅 local 模式使用）
- `HTTP_BODY_LIMIT_MB`：HTTP 请求体大小上限（MiB），默认 `64`（建议保留默认以兼容较大附件的 Base64 上传）
- `MEMOS_VERSION`：`/api/v1/instance/profile` 返回版本，默认 `0.26.1`
- `ALLOW_REGISTRATION`：是否允许公开注册，默认 `true`
- `BOOTSTRAP_USER`：引导用户名，默认 `demo`
- `BOOTSTRAP_TOKEN`：引导令牌，默认空（为空则不创建引导令牌）

说明：

- 存储方式与 S3 配置统一保存在数据库 `system_settings` 中
- 新库默认 `storage_backend=local`
- 可通过运行时控制台 `storage ...` 命令维护存储配置

## 已实现 API

- `GET /api/v1/instance/profile`
- `POST /api/v1/auth/signin`（密码登录，返回 `accessToken`）
- `POST /api/v1/users`（公开接口，兼容 memos CreateUser）
- `GET /api/v1/auth/me`
- `GET /api/v1/users/{name}`（`name` 支持数字 ID 或用户名）
- `GET /api/v1/users/{name}/settings/GENERAL`
- `GET /api/v1/users/{name}:getStats`
- `GET /api/v1/memos`
- `POST /api/v1/memos`
- `PATCH /api/v1/memos/{id}`
- `DELETE /api/v1/memos/{id}`
- `GET /api/v1/attachments`
- `POST /api/v1/attachments`
- `DELETE /api/v1/attachments/{id}`
- `GET /file/attachments/{id}/{filename}`

## 用户注册

兼容 memos 官方 CreateUser 注册接口：

```http
POST /api/v1/users
Content-Type: application/json

{
  "user": {
    "username": "alice_01",
    "displayName": "Alice",
    "password": "alice-password",
    "role": "USER"
  },
  "validateOnly": false
}
```

说明：

- 请求体与官方 `CreateUserRequest` 对齐：`user` + `validateOnly`
- 首个用户始终可创建，且自动赋予 `ADMIN`
- 非首个用户在 `ALLOW_REGISTRATION=false` 时会被拒绝（除非请求方是 `ADMIN/HOST`）
- 普通匿名注册即使传 `role=ADMIN`，也会按 `USER` 创建
- `password` 为必填，禁止空密码
- `validateOnly=true` 时仅校验参数，不落库

## 登录

兼容 memos 官方密码登录请求结构（`passwordCredentials`）：

```http
POST /api/v1/auth/signin
Content-Type: application/json

{
  "passwordCredentials": {
    "username": "alice_01",
    "password": "alice-password"
  }
}
```

成功返回：

- `user`
- `accessToken`（可用于 `Authorization: Bearer <token>`）

## 标签兼容行为

### 解析规则

- 触发符：`#`
- 支持 Unicode 字母
- 支持 Unicode 数字
- 支持 Unicode Symbol（含 emoji）
- 支持 `_`、`-`、`/`、`&`
- `##...` 不算标签
- `# ` 不算标签
- 最大长度：`100` 个 rune
- 大小写敏感去重（`work` 与 `Work` 视为不同标签）
- 去重后保留首次出现顺序
- 层级标签保留原样（如 `work/project`）

### 写入与更新

- `POST /api/v1/memos`：服务端解析 `content` 并重建 payload
- `PATCH /api/v1/memos/{id}` 且 `content` 变更：重建 payload
- `PATCH /api/v1/memos/{id}` 仅改可见性/置顶等：不重算 tags
- 服务端不以客户端传入 tags 作为真实来源
- 兼容 memos 行为：允许空内容 memo（如“仅附件 memo”）

### 统计

- `GET /api/v1/users/{name}:getStats` 的 `tagCount` 基于 `memo.payload_json.tags`
- 同一 memo 中重复标签只计一次（payload 已去重）
- 统计时应用可见性规则

### 过滤

已支持（CEL）：

- `tag in ["book"]`：命中 `book` 与 `book/...`
- `tag in ["book","work"]`：OR 语义
- `tags.exists(t, t.startsWith("book"))`
- `"work" in tags`
- 组合表达式示例：`creator_id == 1 && visibility in ["PRIVATE"] && !("work" in tags)`

说明：

- 为兼容旧语法，`tag in [...]` 会在服务端重写为 CEL 再执行。
- 在执行 CEL 前，会先做一层 SQL 安全下推（不改变语义，只减少候选 memo）。

### CEL 下推策略

当前会下推的典型子表达式：

- `creator_id == ...`
- `creator_id in [...]`
- `visibility == ...` / `visibility in [...]`
- `state == ...` / `state in [...]`
- `pinned == ...` / `pinned in [...]`
- `property.hasLink == ...`
- `property.hasTaskList == ...`
- `property.hasCode == ...`
- `property.hasIncompleteTasks == ...`
- `pinned != ...`
- `visibility != ...`
- `state != ...`
- `property.hasLink != ...`
- `property.hasTaskList != ...`
- `property.hasCode != ...`
- `property.hasIncompleteTasks != ...`
- `"x" in tags`
- `!("x" in tags)`
- `tags.exists(t, t.startsWith("prefix"))`
- `!tags.exists(t, t.startsWith("prefix"))`
- `tag in [...]`（经重写后可下推单标签场景）

对于无法安全下推的 CEL 结构（如复杂 `||`、复杂否定、复杂宏组合），系统会自动回退为“仅 CEL 最终求值”。

已支持部分 `||` 的下推：

- 示例：`creator_id == 1 || creator_id == 2` 可下推为 `creator_id in [1,2]`
- 若某个 `||` 分支无法安全提取约束，则对应字段下推会自动放弃（不影响最终结果正确性）

## 历史数据回填

回填历史 memo 的 payload（tags + property）：

```text
memo rebuild-payload
```

## 运维命令（后台管理）

后端仅支持默认启动方式（`go run ./cmd/server`），并始终开启运行时控制台；运维命令统一在控制台执行。

### 运行时命令模式（服务运行中执行）

默认启动即进入控制台模式：

```powershell
go run ./cmd/server
```

启动后可在同一终端输入命令，例如：

```text
user create alice alice-password
token create alice --ttl 30d
token list alice
registration disable
storage status
```

输入 `help` 查看命令，输入 `exit` 退出控制台（不会停止 HTTP 服务）。

### 1) 后台创建用户

```text
user create <username> <password> [display_name] [role]
```

示例：

```text
user create alice alice-password Alice USER
```

说明：

- `role` 可选，默认 `USER`
- 命令创建用户时使用管理员权限语义，可创建普通用户或管理员用户
- 依然会校验用户名与密码（密码禁止为空）

### 2) 为用户生成 Access Token

```text
token create <username_or_id> [description] [--ttl 7d|24h] [--expires-at 2026-12-31T23:59:59Z]
```

示例：

```text
token create alice "mobile token"
token create 1
token create alice --ttl 30d
token create alice --ttl 720h
token create alice --expires-at 2026-12-31T23:59:59Z
```

说明：

- 可选 `--ttl`：相对当前时间的有效期（支持 `d/day/days` 与 Go duration，如 `7d`、`30d`、`24h`、`30m`）
- 可选 `--expires-at`：绝对过期时间（RFC3339 格式）
- `--ttl` 与 `--expires-at` 不能同时使用
- 过期时间必须晚于当前时间
- 不传过期参数时，token 默认不过期

命令会输出可直接使用的 `accessToken`；若设置了过期时间，也会输出 `expiresAt`。

### 2.1) 查看用户的 Access Token 列表

```text
token list <username_or_id>
```

示例：

```text
token list alice
```

说明：

- 会输出 token 元信息：`id`、`token_prefix`、创建时间、过期时间、撤销时间、最后使用时间、描述
- 出于安全原因，不会输出完整 token 明文

### 2.2) 撤销 Access Token

```text
token revoke <token_id>
```

示例：

```text
token revoke 12
```

说明：

- 撤销后该 token 立即失效
- 再次撤销同一个 token 会提示“已撤销”

### 3) 动态允许/禁止注册

```text
registration status
registration enable
registration disable
```

说明：

- 该开关持久化在数据库中，修改后立即影响 `POST /api/v1/users` 行为
- 若数据库中没有该设置，则回退到环境变量 `ALLOW_REGISTRATION`

### 4) 动态配置存储后端（数据库持久化）

```text
storage status
storage set-local
storage wizard
storage set-s3 `
  --endpoint "https://<你的S3地址>" `
  --region "auto" `
  --bucket "memos" `
  --access-key-id "<access-key-id>" `
  --access-key-secret "<access-key-secret>" `
  --use-path-style=true
```

说明：

- 以上配置会写入 `system_settings` 表
- `storage wizard` 会以交互方式逐项提示输入 S3 配置
- 也可使用 `set-s3 --interactive` 进入交互模式（可搭配部分参数预填默认值）
- `storage status` 会显示当前生效的存储配置（密钥会脱敏展示）
- 修改后端类型后需要重启服务，新的存储实现才会生效

## 测试

```powershell
go test ./...
go vet ./...
```

测试已覆盖：

- 标签解析（中日韩、emoji、`/`、`&`、标题排除、100 rune 上限、大小写敏感去重）
- tagCount 聚合逻辑
- `tag in [...]` 层级过滤
- CEL 过滤与 SQL 下推提取（含 `tags.exists`、`"x" in tags`、冲突约束）
- Fiber 路由对 `{id}:getStats` 的匹配兼容性

## 说明

- 本项目是面向 MoeMemos Android 联调的增量兼容后端
- 不是 `usememos/memos` 的完整替代实现

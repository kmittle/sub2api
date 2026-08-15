# Quota Recovery Development Handoff

更新日期：2026-08-16

当前开发分支：`handoff/quota-recovery-20260815`

当前基线提交：`7e5fc83d0`

## 最终语义

在 Sub2API 后端原生实现账号额度恢复巡检：服务启动后立即执行一次，之后对齐 UTC
`00:00/04:00/08:00/12:00/16:00/20:00`，每 4 小时执行一次。

巡检负责两件事：

- 使用上游原生额度接口刷新已有额度/余额快照；没有原生额度接口的凭据只做只读连通性探测，不虚构余额。
- 探测确认额度可用后，按观测代次清除额度派生的数据库、Redis 和内存阻断。

`accounts.schedulable` 完全归管理员所有。额度耗尽、429、自动巡检和额度恢复均不得修改该字段，也不得把它作为是否刷新额度状态的前置条件。

示例：管理员在账号额度耗尽后把 `schedulable` 关闭。额度恢复时，巡检只刷新额度快照并清除 429/额度阻断，账号仍保持人工关闭；只有管理员以后手工重新开启，账号才会重新参与调度。

## 原始指令与澄清

最初指令使用了“刷新账号的可调度状态”这一表述。后续已明确：这里真正需要恢复的是额度/限流状态，不是管理员维护的 `schedulable` 开关。本文其他章节均以澄清后的最终语义为准。

原始指令如下：

> 写一个 cron，每 4 小时检查一次 sub2API 的所有账号额度是否重置或者有新的额外额度可用，总之就是账号可用了，然后刷新账号的可调度状态，并且刷新账号余额显示。API 类似，检查 API 是否有可调用额度，有则重置 API 的可调度状态，并且刷新 API 余额显示；如果 sub2API 原生不能展示 API 余额，则不新增虚构展示。

## 当前实现

基础巡检实现来自提交 `1ab2c83f4122d7fd6199d30617e74e9466f0f57f`：

- Redis/PostgreSQL 单例锁配合 `settings.quota_recovery_last_utc_slot` 持久化槽位认领，避免多实例重复执行。
- 每页 100 个账号，最多 2 个并发探测，单周期超时 45 分钟。
- Anthropic OAuth、OpenAI OAuth、Antigravity OAuth、Grok OAuth 使用现有原生额度接口并刷新已有 UI 快照字段。
- API Key、Setup Token、Bedrock、Vertex Service Account、Gemini 和 Upstream 等没有原生额度接口的凭据只执行只读连通性探测。
- 服务已接入 Wire 启停生命周期；成功周期记录 `quota_recovery_cycle_completed`。

### 额度阻断状态

现有字段可以无迁移地区分普通限流和恢复时间未知的额度耗尽：

```text
健康/未限流：
rate_limited_at = NULL
rate_limit_reset_at = NULL

普通带恢复边界的 429：
rate_limited_at != NULL
rate_limit_reset_at != NULL

额度耗尽且恢复时间未知：
rate_limited_at != NULL
rate_limit_reset_at = NULL
```

第三种状态必须由权威额度查询或成功的只读连通性探测清除，不能因为本地时间经过而自动失效。所有主要调度 SQL、内存调度判断、管理端筛选、分组统计、Ops 统计和 dashboard 统计都已识别并排除/展示该状态。

`RateLimitService` 现在按如下方式处理余额耗尽：

- Anthropic `400` 且错误信息包含 `credit balance` 时写入无限期额度阻断。
- 普通 `402` 写入无限期额度阻断；OpenAI `deactivated_workspace` 仍是永久账号错误，不属于额度恢复。
- 写入额度阻断时保持原有 `status` 和 `schedulable`，OpenAI 内存阻断原因使用专用的 `quota_exhausted`。
- 后续普通有限期 429 不得覆盖或削弱已有无限期额度阻断。

### 旧数据兼容

旧版 `RateLimitService` 会把余额耗尽写成以下错误之一：

```text
status=error
Credit balance exhausted (400): ...
```

```text
status=error
Payment required (402): ...
```

`ClearQuotaError` 只用于兼容这两个固定前缀：

- 候选扫描不要求旧数据的 `schedulable` 为 true 或 false。
- 成功探测后只恢复 `status=active` 并清空 `error_message`，保留数据库中当时的 `schedulable` 值。
- 认证、权限、entitlement、停用、自定义错误和其他一般 `status=error` 账号不会进入自动恢复。
- 旧版曾把额度错误的内存阻断标成 `auth_error` 或 `upstream_disable`；新版巡检不会宽泛清理这两类原因，只清理专用的 `quota_exhausted`。

### 人工调度开关不变量

- 候选扫描会纳入人工关闭但仍带有限期 429、无限期额度阻断或额度阈值阻断的账号。
- 原生额度账号即使人工关闭，也继续刷新额度展示。
- `ApplyQuotaRecoveryMutation` 的 SQL 不读取、不比较、不写入 `schedulable`。
- 探测期间管理员并发关闭或开启调度，不会阻止额度快照和观测到的额度状态更新，也不会丢失管理员的新值。
- OpenAI shadow 的凭据 parent 必须仍为 `active` 且凭据身份未变化；parent 是否人工可调度不影响 shadow 自身额度刷新。
- `disabled`、非额度 `error`、已删除或按策略过期的账号仍不会被恢复。

### CAS 与状态清理

数据库 mutation 在单条 SQL 中合并展示快照并清理指定阻断。CAS 继续约束：

- target 和 credential owner 的 ID、platform、type、status、完整 credentials、proxy、parent identity；
- TLS fingerprint、quota dimension、`allow_overages` 等会影响探测身份或额度解释的配置；
- 有限期 429、无限期额度阻断、阈值阻断和模型级限流各自的完整观测代次。

当前 mutation 参数末段为：

```text
$38  ClearQuotaError（旧固定余额错误兼容）
$39  探测前的完整 error_message
$40  StatusActive
$41  ClearQuotaExhaustion（无限期额度阻断）
$42  探测前的 rate_limited_at
```

CAS miss 不刷新 scheduler snapshot，不删除 Redis 状态，也不清理内存阻断。数据库更新成功后，Redis 和 OpenAI 内存状态仍使用完整观测代次做条件式删除。

空模型限流键集合必须规范化为非 nil 空切片，确保 `pq.Array` 向 PostgreSQL 传空数组而不是 `NULL`，避免误清账号原有 `extra`。

### 前端展示

- `rate_limited_at != NULL && rate_limit_reset_at == NULL` 显示为 429/额度耗尽。
- 不显示虚构的自动恢复时间，改为“等待额度巡检确认恢复”。
- 管理端本地筛选、操作菜单和额度信息组件使用与后端相同的判断。

## 主要文件

```text
backend/internal/repository/account_repo.go
backend/internal/repository/account_repo_quota_recovery.go
backend/internal/repository/account_repo_quota_recovery_integration_test.go
backend/internal/repository/account_repo_quota_exhaustion_integration_test.go
backend/internal/service/account.go
backend/internal/service/openai_account_runtime_block_fastpath.go
backend/internal/service/quota_recovery_service.go
backend/internal/service/ratelimit_service.go
backend/internal/service/ratelimit_service_quota_exhaustion_test.go
frontend/src/components/account/AccountQuotaInfo.vue
frontend/src/components/account/AccountStatusIndicator.vue
frontend/src/views/admin/AccountsView.vue
```

## 本轮本地验证

本轮只使用本地单元测试、Testcontainers PostgreSQL/Redis 和前端测试桩；没有连接中转站，没有调用真实账号/API，没有启用 VPN。

后端严格串行执行：

```bash
/usr/local/bin/go test -tags=unit -p=1 ./... -count=1
sudo -u bycao -g docker -- /usr/local/bin/go test -tags=integration -p=1 ./... -count=1
golangci-lint run --concurrency=1 ./...  # v2.9.0, 0 issues
/usr/local/bin/go build -p=1 ./cmd/server
```

前端使用仓库默认 Node 24 镜像、锁定的 pnpm 9.15.9 依赖和 4096 MiB 临时 V8 堆执行：

```text
ESLint：通过
vue-tsc --noEmit：通过
Vitest：222 files / 1549 tests 通过
vue-tsc -b：通过
vite build：通过
```

此外已执行定向 service/repository 单元测试、真实 PostgreSQL quota recovery 集成测试、前端额度状态定向 Vitest、`gofmt` 和 `git diff --check`。

## 重点审阅项

- 全仓搜索额度恢复 SQL，不应存在写入 `schedulable` 的语句。
- `ListQuotaRecoveryAccountPage` 对一般 `status=error` 必须保持拒绝，只允许两个固定旧余额错误前缀。
- 人工关闭账号仍应进入额度状态恢复，但 mutation 后必须继续保持关闭。
- 成功且能证明额度可用的探测才允许清理；失败、超时、空响应或非 success 必须保留原状态。
- 有原生额度接口的账号必须刷新真实快照；没有原生接口的账号不得写伪造余额。
- 新的 quota generation、凭据、代理、parent、TLS、quota dimension 或 overages 配置必须让旧 CAS 失败。
- OpenAI 运行时清理只接受与观测代次一致的 `429`、`429_fallback`、`account_scheduling_threshold` 或 `quota_exhausted`，不得清理认证和人工策略原因。
- 分组绑定、分组优先级、模型映射、effort 映射、credentials 和无关 `extra` 必须保持不变。

## 历史部署记录

2026-08-15 曾构建并部署镜像 `local/sub2api:quota-recovery-20260815-f480d02c9`，其镜像 ID 为 `sha256:0a268217e28534bf8399998b231acd41ae3ad8bd37ff5a5875faa8b2cc8a194a`。该镜像对应澄清前实现，会在旧余额错误恢复时自动写 `schedulable=true`，因此不能作为最终语义的验收镜像，也不能直接复用为本次修复的发布产物。

当时的 Compose 五层叠加、分组、模型映射、effort 映射和代理拓扑均未改动；旧运行镜像 `local/sub2api:claude-idle-20260814-r2` 曾保留作回滚。当前修复必须在本地验证完成后重新提交、构建新镜像并部署，不能只重启旧镜像。

## 部署边界

- 合并和部署不得修改中转站现有分组、模型映射、effort 映射、凭据、代理拓扑或 Compose 叠加顺序。
- 中转站内存较小，不在那里执行 Go、Node 或 Docker build；应在当前开发服务器构建并校验镜像，再传输镜像归档。
- 部署前记录当前 Git 提交、镜像 ID、归档 SHA256 和 Compose 展开结果；部署后验证健康检查、巡检周期日志、scheduler outbox、账号快照和宿主资源。
- 真实 Kimi API 余额有限；除非确定性本地测试无法覆盖，不重复消耗真实额度。

# Quota Recovery Development Handoff

更新日期：2026-08-15

远端分支：`fork/handoff/quota-recovery-20260815`

## 目标

在 Sub2API 后端原生实现账号额度恢复巡检：服务启动后立即执行一次，之后按 UTC 每 4 小时执行一次。发现账号额度恢复或新增可用额度后，刷新账号的原生额度/余额快照，并只在可用性探测成功后恢复对应账号的可调度状态。

没有原生额度查询接口的 API Key、Setup Token、Bedrock、Vertex Service Account、Gemini 及 Upstream 凭据只做只读连通性探测，不虚构余额展示。

## 原始指令（写这个文档的agent接收到的原始指令）

写一个cron，每4小时检查一次sub2API的所有账号额度是否重置或者有新的额外的额度可用，总之就是账号可用了，然后刷新账号的可调度状态，并且刷新账号余额显示
API类似，检查API是否有可调用额度，有则重置API的可调度状态，并且刷新API余额显示（我忘了sub2API是否原生就能展示API余额，如果不能就不加展示余额的功能）

以上功能不一定以cron形式实现，也可以直接实现在Sub2API中，例如，使用Go语言，或者使用python脚本等，你选择最合适的实现方式

之前可能已经基于cron实现过，但是可能没完成，如果你打算继续使用cron实现可以看看原来实现的进展；如果你不打算采取这个技术路线可以清除之前使用cron实现路线的dirty改动。

是否完全理解？

## 当前实现状态

基础实现位于提交 `1ab2c83f4122d7fd6199d30617e74e9466f0f57f`：

- `QuotaRecoveryService` 启动时立即运行，随后对齐 UTC `00:00/04:00/08:00/12:00/16:00/20:00`。
- Redis/PostgreSQL 单例锁配合 `settings.quota_recovery_last_utc_slot` 持久化槽位认领，避免多实例重复执行。
- 每页 100 个账号，最多 2 个并发探测，单周期超时 45 分钟。
- Anthropic OAuth、OpenAI OAuth、Antigravity OAuth、Grok OAuth 使用现有原生额度接口并刷新已有 UI 快照字段。
- 其他受支持凭据通过 `AccountTestService` 的只读模式执行连通性探测，不写测试副作用，也不新增余额字段。
- 数据库 mutation 对账号、凭据 owner、代理、凭据、限流代次和调度策略做 CAS；探测期间状态发生变化时不清理新状态。
- Redis 临时不可调度状态和 OpenAI 内存阻断也按观测代次条件清理，不使用宽泛删除。
- 服务已经接入 Wire 启停生命周期，成功周期会记录 `quota_recovery_cycle_completed`。

余额错误恢复位于提交 `966ad85ca`：

基础实现遗漏了 `RateLimitService` 通过 `SetError` 持久化的余额耗尽账号。这类账号会被写成：

```text
status=error
schedulable=false
Credit balance exhausted (400): ...
```

或：

```text
status=error
schedulable=false
Payment required (402): ...
```

原扫描只选择 `status=active`，因此这些账号永远无法进入恢复流程。本分支修复如下：

- 仅识别 `RateLimitService` 写入的两个固定错误前缀，不匹配认证、权限、停用或自定义错误。
- 将上述 `error + schedulable=false` 账号加入候选扫描。
- 可用性探测成功后设置 `ClearQuotaError`，通过单条 SQL CAS 原子恢复为 `status=active`、清空 `error_message` 并设置 `schedulable=true`。
- CAS 要求错误文本仍与探测前完全一致，避免清除探测期间产生的新错误。
- OpenAI shadow 账号只有在凭据 parent 仍为 `active + schedulable` 时才能恢复。
- 认证错误、entitlement 错误、人工暂停、disabled 和过期账号仍不会自动恢复。
- OpenAI 运行时阻断清理增加显式 `clearQuotaError` 参数；仅允许在该标志下按代次清理 `auth_error` 或 `upstream_disable`。

真实 E2E 随后发现：没有模型限流键时，`pq.Array(nil)` 会向 PostgreSQL 传入 `NULL`，导致 mutation 把账号原有 `extra` 清空。提交 `c693bcc23` 将空键集合规范化为非 nil 空切片，确保 SQL 收到空数组；repository integration 测试同时断言 `extra.operator_setting` 在余额错误恢复后仍保留。

本功能主要修改文件：

```text
backend/internal/repository/account_repo_quota_recovery.go
backend/internal/repository/account_repo_quota_recovery_integration_test.go
backend/internal/repository/account_repo_quota_recovery_test.go
backend/internal/service/openai_account_runtime_block_fastpath.go
backend/internal/service/openai_account_runtime_block_fastpath_test.go
backend/internal/service/quota_recovery_service.go
backend/internal/service/quota_recovery_service_test.go
```

## 已完成验证

- 已执行 `gofmt`。
- `git diff --check` 通过。
- 两条 repository SQL 已在生产 PostgreSQL 上通过 `PREPARE` / `DEALLOCATE` 语法和参数类型校验；mutation 参数覆盖到 `$43`。
- 已静态检查 `ClearQuotaRecoveryRuntimeBlock` 的所有实现和调用点，均已同步第五个参数。
- 已新增 service、runtime blocker、repository unit 和 repository integration 测试。
- 在内存更充足的开发服务器上，最终工作树已严格串行通过：

```bash
/usr/local/bin/go test -tags=unit -p=1 ./... -count=1
sudo -u bycao -g docker -- /usr/local/bin/go test -tags=integration -p=1 ./... -count=1
golangci-lint run ./...  # v2.9.0, 0 issues
/usr/local/bin/go build ./cmd/server
```

- 全量测试暴露了一条旧 Messages fallback 用例的错误预期：仓库既有映射会把上游 `503` 转为客户端 `502`，测试却期待 `503`。只修正了测试预期，未修改生产错误映射。
- 为恢复 lint 基线，还机械修正了 Kimi membership relay 的 `Close` 返回值处理、测试类型断言，并删除两个已被带协议信息的新入口替代的私有未使用包装函数；Kimi 定向测试和全量测试均通过。
- 当前分支没有修改 Wire provider 关系；基础提交已包含 `wire_gen.go`，本轮无需重新生成 Wire。
- 生产镜像的默认 Dockerfile 将前端 V8 堆固定为 1536 MiB，本次 `vue-tsc` 在该上限 OOM。最终构建只使用临时 Dockerfile 将构建阶段堆提高到 4096 MiB；仓库 Dockerfile 和最终运行环境未改动。
- Docker Hub 直连拉取基础镜像超时，最终仅通过构建参数使用 DaoCloud 镜像源；没有启用 VPN 或修改 Docker daemon/系统代理。

## 真实 E2E

- 本地配置由生产数据库重新脱敏导出；规范化 OAuth 到期时间、运行时代理、Kimi relay 和动态健康字段后，本地与中转站快照的 498 个静态叶子字段完全一致。
- 真实 Kimi API 只执行过一次最小 `hi` 请求：固定 400 余额错误账号成功恢复为 `active + schedulable`，且没有写入伪造余额字段。因 API 余额有限，发现并修复 `extra` 清空问题后没有再次调用真实 Kimi API。
- 修复后的最终确定性 E2E 使用 3 个 Codex OAuth 原生额度查询、1 个本地成功账号和 1 个本地失败账号；周期日志为：

```text
accounts_scanned=5 probes_attempted=5 snapshots_saved=3 blocks_cleared=1 errors=1
```

- 成功账号恢复，失败账号保持原固定错误；两个本地测试账号的 `extra` 哈希在巡检前后完全一致。
- 三个 Codex OAuth 的 5h/7d 原生快照均刷新，生产模式字段保留。
- 认领 UTC 槽位 `2026-08-14T20:00:00Z`；同槽重启没有再次巡检，状态哈希不变。

## 重点审阅项

- `ListQuotaRecoveryAccountPage` 必须只纳入两个固定余额错误前缀，不能把一般 `status=error` 账号加入自动恢复。
- `ApplyQuotaRecoveryMutation` 中 `$39` 到 `$43` 的 quota-error CAS 必须同时约束 target 错误代次和 credential identity 状态。
- OpenAI shadow 的 target 与 credential parent 不同；parent 人工暂停或非 active 时不得恢复 shadow。
- 成功探测才允许 `ClearQuotaError`。失败、超时、空响应或非 success 的连通性测试必须保持原状态。
- API Key 等没有原生余额接口的账号不得写入伪造额度快照。
- 清理数据库状态成功后，运行时/Redis 清理仍必须是条件式操作；CAS miss 不得刷新 scheduler snapshot 或清理缓存。

## 镜像与生产部署

- 本机构建并验证镜像 `local/sub2api:quota-recovery-20260815-f480d02c9`：

```text
image_id=sha256:0a268217e28534bf8399998b231acd41ae3ad8bd37ff5a5875faa8b2cc8a194a
archive_sha256=dda92d6625e71a1584d8521e02a65b604441034c67e0ce35858e2b79281ff240
embedded_version=quota-recovery-20260815
embedded_commit=f480d02c9
```

- 中转站只执行 Git fast-forward、镜像归档校验、`docker load` 和 Compose 重建，没有运行 Go、Node 或 Docker build。
- Compose 保持原有五层叠加：`docker-compose.yml`、`docker-compose.server.yml`、`docker-compose.network.yml`、`docker-compose.composite.yml`、`docker-compose.kimi-membership.yml`。
- `/home/ubuntu/sub2api/deploy/.env` 的唯一 `SUB2API_CUSTOM_IMAGE` 已持久化为新镜像；其余服务及固定镜像 digest 未改变。
- 旧镜像 `local/sub2api:claude-idle-20260814-r2`（ID `26973eacd465`）仍保留，可直接回滚。

## 生产验证

- 上线前候选统计只有 3 个 OpenAI OAuth；没有 Kimi/API Key 候选，因此部署和受控验证均未调用 Kimi API。
- 首次周期发现账号 1 的旧 Codex access token 已被 OpenAI 作废；另外两个账号正常刷新。相同的新凭据在本地 OpenAI 通道验证成功后，生产通道仍拒绝该 access token，因此最终在生产 Mihomo 出口内强制走既有 refresh-token 流程，未改变代理拓扑。
- 凭据同步和到期时间调整均使用账号身份、状态及完整凭据哈希 CAS；敏感 JSON 只经 SSH 管道传输、不落盘、不回显。对应 scheduler outbox 均已消费，Redis 快照已更新。
- 最终生产周期：

```text
slot=2026-08-15T00:00:00Z
accounts_scanned=3 probes_attempted=3 snapshots_saved=3 blocks_cleared=0 errors=0
```

- 三个 Codex OAuth 均有原生额度快照；一个尚未恢复的限流代次继续保留，没有错误清理或越权恢复。
- 普通同槽重启已验证不会重复执行；受控验证期间曾精确删除槽位以主动复测，最终槽位已恢复为 `2026-08-15T00:00:00Z`，之后不再人工重置。
- `sub2api`、Mihomo、PostgreSQL、Redis、Kimi membership relay 和 Caddy 全部 healthy；容器内及公网 `https://kmittle.cloud/health` 均返回 `{"status":"ok"}`。
- 最终宿主指标：可用内存约 2620 MiB，Swap 使用 129 MiB，memory PSI `some/full` 的 10/60/300 秒均为 `0.00`；`sub2api` 内存约 82 MiB。
- 最终日志无 quota recovery、scheduler、panic 或 fatal 错误。既有 URL allowlist/trusted proxy/CORS 配置告警及活跃 WebSocket 请求取消告警不属于本功能回归。

## 未纳入分支的本机文件

以下三个文件是开发开始前已有的本机备份，不属于 quota recovery，保持未跟踪且不会推送：

```text
deploy/docker-compose.kimi-membership.yml.bak.20260814-175245
deploy/docker-compose.network.yml.bak.20260814-191613
deploy/docker-compose.server.yml.bak.20260814-201747
```

# Quota Recovery Development Handoff

更新日期：2026-08-15

远端分支：`fork/handoff/quota-recovery-20260815`

## 目标

在 Sub2API 后端原生实现账号额度恢复巡检：服务启动后立即执行一次，之后按 UTC 每 4 小时执行一次。发现账号额度恢复或新增可用额度后，刷新账号的原生额度/余额快照，并只在可用性探测成功后恢复对应账号的可调度状态。

没有原生额度查询接口的 API Key、Setup Token、Bedrock、Vertex Service Account、Gemini 及 Upstream 凭据只做只读连通性探测，不虚构余额展示。

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

## 本分支新增修复

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

本轮修改文件：

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

## 尚未完成验证

本分支新增修复尚未成功完成 Go 类型检查、测试编译或服务构建。原因是原服务器只有约 3.6 GiB RAM，相关 Go 包体积很大：

- 先前测试容器曾在 800/900 MiB 和约 1.27 GiB 限额下触顶，引发宿主 memory PSI 和 Swap 压力。
- 最后一次 `go/packages.LoadSyntax` 轻量尝试限制为 500 MiB、550 MiB memory+swap、1 CPU、无网络；约 8 秒后达到 499.8 MiB，并出现少量新增 swap-out，已立即停止。
- 临时类型检查容器已经删除，临时 `backend/tmp_quota_typecheck.go` 也未纳入分支。

因此，接手者必须把“测试已编写”和“测试已通过”区分开，不应在完成下列验证前部署。

## 新服务器接手步骤

所有 Go 测试、构建和 Wire 操作继续严格串行。不要同时运行 service/repository 测试，也不要让测试与构建重叠。

```bash
git fetch fork
git switch --track fork/handoff/quota-recovery-20260815
cd backend
gofmt -w internal/repository/account_repo_quota_recovery.go \
  internal/repository/account_repo_quota_recovery_integration_test.go \
  internal/repository/account_repo_quota_recovery_test.go \
  internal/service/openai_account_runtime_block_fastpath.go \
  internal/service/openai_account_runtime_block_fastpath_test.go \
  internal/service/quota_recovery_service.go \
  internal/service/quota_recovery_service_test.go
git diff --check
```

先逐条运行本轮新增的重点测试；每条命令结束后再启动下一条：

```bash
go test ./internal/service -run 'TestQuotaRecovery(BalanceError|IdentityAllowsRestore|ProbeEligibility)|TestOpenAIRuntimeBlock_QuotaErrorClear' -count=1
go test ./internal/repository -run 'TestApplyQuotaRecoveryMutationRejectsNonBalanceAccountError' -count=1
go test ./internal/repository -run 'TestApplyQuotaRecoveryMutationRestoresOnlyExactBalanceErrorGeneration|TestListQuotaRecoveryAccountPageIncludesBalanceAndConnectivityCandidates' -count=1
```

然后依次扩大验证范围：

```bash
go test ./internal/service -run 'TestQuotaRecovery|TestOpenAIRuntimeBlock_QuotaRecovery|TestOpenAIRuntimeBlock_QuotaErrorClear' -count=1
go test ./internal/repository -run 'TestQuotaRecovery|TestApplyQuotaRecoveryMutation|TestListQuotaRecoveryAccountPage' -count=1
go test ./internal/service -count=1
go test ./internal/repository -count=1
go build ./cmd/server
```

当前分支没有修改 Wire provider 关系；基础提交已经包含更新后的 `wire_gen.go`，因此正常验证不需要运行完整 Wire 图分析。只有在接手开发继续修改依赖注入后才重新生成 Wire。

## 重点审阅项

- `ListQuotaRecoveryAccountPage` 必须只纳入两个固定余额错误前缀，不能把一般 `status=error` 账号加入自动恢复。
- `ApplyQuotaRecoveryMutation` 中 `$39` 到 `$43` 的 quota-error CAS 必须同时约束 target 错误代次和 credential identity 状态。
- OpenAI shadow 的 target 与 credential parent 不同；parent 人工暂停或非 active 时不得恢复 shadow。
- 成功探测才允许 `ClearQuotaError`。失败、超时、空响应或非 success 的连通性测试必须保持原状态。
- API Key 等没有原生余额接口的账号不得写入伪造额度快照。
- 清理数据库状态成功后，运行时/Redis 清理仍必须是条件式操作；CAS miss 不得刷新 scheduler snapshot 或清理缓存。

## 部署后验证

当前生产仍运行镜像 `local/sub2api:claude-idle-20260814-r2`，健康状态正常；quota recovery 新实现尚未部署。

完成构建和测试后，按该环境既有发布流程串行构建并替换服务。启动后检查：

1. 新镜像和新容器均处于 healthy。
2. 启动周期出现 `quota_recovery_cycle_completed`，并核对 `accounts_scanned`、`probes_attempted`、`snapshots_saved`、`blocks_cleared`、`errors`。
3. PostgreSQL 中存在 `settings.key = 'quota_recovery_last_utc_slot'`，值为当前 UTC 四小时槽位。
4. 选择一个明确由 400/402 余额错误产生的测试账号，确认失败探测不改变状态，成功探测才恢复 `active + schedulable`。
5. Anthropic/OpenAI/Antigravity/Grok OAuth 的原生额度展示已刷新；API Key 账号没有新增虚构余额。
6. 观察宿主 RAM、Swap、memory PSI、服务日志和所有生产容器健康状态。

## 未纳入分支的本机文件

以下三个文件是开发开始前已有的本机备份，不属于 quota recovery，保持未跟踪且不会推送：

```text
deploy/docker-compose.kimi-membership.yml.bak.20260814-175245
deploy/docker-compose.network.yml.bak.20260814-191613
deploy/docker-compose.server.yml.bak.20260814-201747
```

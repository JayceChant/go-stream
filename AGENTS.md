# AGENTS.md — go-stream 项目协作规范

> 本文件是**每一次开发循环**都必须遵守的全局规则，适用于所有参与者（人类与 AI agent）。
> 开始任何工作前，先完整阅读本文件及下述引用文件。

## 0. 规范组成（本文件 + 引用文件）

本项目的协作规范由三部分组成，**全部必须遵守**；语言无关与语言相关部分抽至 [`.agents/`](./.agents/) 子目录，便于复用到其它项目：

- [`.agents/general.md`](./.agents/general.md) —— **语言无关的通用规范**：规格驱动流程与需求收敛门禁、Git 中文 Conventional Commits、Git 身份卫生、Shell 执行环境优先级（WSL → Git Bash → PowerShell）、`.env.local` 环境中立、清单勾选纪律
- [`.agents/go.md`](./.agents/go.md) —— **Go 语言规范与质量门槛**：Go 1.27 泛型方法三条硬限制、错误即值、`go fix` 现代写法采纳清单、覆盖率/竞态/性能/Fuzz 门槛与提交前命令（`go fix`/`gofmt`/`go vet`/`go test`/`golangci-lint`）
- 本文件 —— **项目专属内容**：`.spec/` 结构、文件布局、scope 常用值、GitHub CI/发版约定、下游使用指引同步

## 1. 规格文档（.spec/）

- 规格文档体系口径见 [`.agents/general.md`](./.agents/general.md) 第 1 节：
  - [`.spec/README.md`](./.spec/README.md) —— 索引与全局约定（组织方式、EARS/ADR 边界、编写格式；非规范）
  - [`.spec/requirements.md`](./.spec/requirements.md) —— EARS 行为契约（Requirement SHALL + Scenario WHEN/THEN + 决策依据）；根级位置即全局作用域
  - [`.spec/decisions.md`](./.spec/decisions.md) —— ADR 决策史（背景/备选/后果，按日期倒序，**不作为实现依据**）
  - [`.spec/tasks.md`](./.spec/tasks.md) —— 任务清单（未勾选 = 待执行，勾选 = 已验收）
  - [`.spec/checklist.md`](./.spec/checklist.md) —— 固定验收动作（有序核对，不勾选）
- 本项目为小型单域库，`.spec/` **只保留根级五文件，不建域目录**；拆分触发条件（任一文件 >300 行，或出现两个以上独立域并导致并行编辑冲突）见 `.spec/README.md`。
- **EARS 不锁死方案**：`requirements.md` 只写可观测行为与跨边界契约，须过「删除测试」（删掉实现名词后仍可判定真伪）；技术选型、内部组件/函数、文件组织、工具链配置一律入 `decisions.md` 或 `docs/`。公开 API 名称/签名/返回语义属于跨边界契约，可留在 requirements。
- **spec 驱动开发**：行为类变更一律走「讨论 → 写入 `.spec/tasks.md` → 确认 → 修改代码 → checklist 验收 → 勾选 tasks」循环。「讨论」步 SHALL 先过需求收敛门禁（核心对象 / In-Out Scope / 成功信号 / 关键未知，细则见 `.agents/general.md` 第 1 节），未放行不得起草方案；需求盲区由用户裁决并落「决策依据」，代理不得以自身假设代答。行为类变更的提交/PR SHALL 同批携带对应 spec 文件改动。
- **精简纪律**：`.spec/` 各规格文件目标 ≤300 行，超出时按 `.agents/general.md` 1.1 节处理；`.spec/tasks.md` 保持「未完成 + 最近完成 + 长远影响」最小集；决策史入 `decisions.md`，不设 legacy 档案。精简动作随当次任务提交，不必单独立任务。
- 实现与 spec 冲突时，先修订 spec 并获得确认，再实现；不得绕过 spec 私自变更行为。有代码改动的任务，收尾时勾选 `.spec/tasks.md`（先勾选后提交）并逐条执行 `.spec/checklist.md`。

## 2. 项目专属约定

### 2.1 文件布局

新增文件需在本节补记。原则：只拆与入口包低耦合的部分，无法干净拆分的不强行划分。

- 核心类型与引擎：`stream.go`（Stream/KV/约束别名）、`pipeline.go`、`sink.go`、`spliterator.go`、`op.go`、`sources.go`、`construct.go`、`parallel.go`、`lifecycle.go`（OnClose/Close/Cache）
- 算子：`ops_stateless.go`（含 Err 变体与 MapToNumber）、`ops_stateful.go`（含 Scan/Chunk）、`op_ext.go`（Zip/Enumerate/WindowSliding/Join/LeftJoin）、`numeric.go`（包级数值族）、`number_stream.go`、`time_window.go`（TimeWindow/SortedByTime/CompleteTimeBuckets）、`terminal.go`（含 Err()/ToSeq 与并行终端）
- 子包：`constraints/constraints.go`、`collector/collector.go`、`collector/collector_combine.go`（组合生态）
- 测试：`*_test.go`、`example_test.go`、`benchmark_test.go`、`whitebox_test.go`、`fuzz_test.go`、`coverage_extra_test.go` 等
- 示例：`example/`（独立 module + replace 指向根模块，隔离覆盖率）——`basics`/`collectors`/`numeric`/`errors`/`parallel`/`lifecycle`/`join`/`extensions`/`timewindow`
- 文档：`README.md`/`README_CN.md`、`docs/design.md`（架构原理）、`docs/api.md`（API 参考）、`skills/go-stream/SKILL.md`（面向下游 coding-agent 的使用指引，英文编写、与 API 面同步维护）
- 协作规范：`AGENTS.md`、`.agents/general.md`、`.agents/go.md`（`.agents/` 下两文件可整体复用到其它项目）
- CI/质量与发布：`.github/workflows/{ci,govulncheck,scorecard,sonarcloud,codeql,release-please}.yml`、`codecov.yml`、`sonar-project.properties`（Actions 测试矩阵 + lint、Codecov、govulncheck、OpenSSF Scorecard、SonarCloud 质量门禁、CodeQL）、`release-please-config.json`、`.release-please-manifest.json`、`CHANGELOG.md`（release-please 基于 Conventional Commits 自动维护，v0.1.0 及之前条目人工回填）

### 2.2 其它约定

- commit `scope` 常用值：stream / pipeline / splitterator / collector / spec / docs 等。
- 依赖最小化：不引入第三方运行时依赖（测试工具除外）。
- 不过度设计：不实现 spec 未要求的功能（非目标清单见 `.spec/requirements.md`「非目标」）。
- **coding-agent 使用指引同步维护**：本仓库向下游用户提供 [`skills/go-stream/SKILL.md`](./skills/go-stream/SKILL.md)。新增或变更公开 API（含语义/签名调整）时，必须同步更新该文件对应条目（含「Method vs package-level」清单与内置收集器列表），确保其与实际 API 面一致；不向下游文档暴露 `.spec/` 内部路径。
- 公开 API 变更同时同步中文 godoc、`docs/api.md`、`docs/design.md`；用户可见行为变化同步 `README.md`/`README_CN.md`。
- CI/发版走 GitHub Actions（`.github/workflows/`）与 release-please Release PR 流程；未经用户要求不得改流水线或绕过 release-please 直接打 tag。
- Go 语言规范与质量门槛见 [`.agents/go.md`](./.agents/go.md)。

## 3. 每次任务循环（Loop）流程

1. 读本文件、`.agents/general.md`、`.agents/go.md` 与 `.spec/`（含任务清单），确认任务范围与验收标准；新提出的行为类变更先过需求收敛门禁再进入本循环。
2. 按 `.agents/general.md` 第 3 节选择 shell 环境。
3. 按 spec 与现有目录归属实现；最小改动，不做 spec 之外的发挥。
4. **有代码改动时**：跑 `.agents/go.md` 第 3 节质量门槛，全绿后勾选 `.spec/tasks.md` 对应项。
5. 按提交纪律提交（一个被接受的变更对应一次提交）；勾选与提交的次序、循环结束检查见 `.agents/general.md` 第 8 节。

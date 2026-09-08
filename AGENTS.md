# AGENTS.md — go-stream 项目协作规范

> 本文件是**每一次开发循环**都必须遵守的全局规则，适用于所有参与者（人类与 AI agent）。
> 开始任何工作前，先完整阅读本文件及下述引用文件。

## 0. 规范组成（本文件 + 引用文件）

本项目的协作规范由三部分组成，**全部必须遵守**；语言无关与语言相关部分抽至 [`agents/`](./agents/) 子目录，便于复用到其它项目：

- [`agents/common.md`](./agents/common.md) —— **语言无关的通用规范**：规格驱动流程、Git 中文 Conventional Commits 提交规范、Shell 执行环境优先级（WSL → Git Bash → PowerShell）、`.env.local` 环境中立约定、标准开发循环
- [`agents/go.md`](./agents/go.md) —— **Go 语言规范与质量门槛**：Go 1.27 泛型方法三条硬限制、错误即值、`go fix` 现代写法采纳清单、提交前质量检查命令（`go fix`/`gofmt`/`go vet`/`go test`/`golangci-lint`）
- 本文件 —— **项目专属内容**：spec 目录结构、scope 常用值、项目历史决策、文件布局约定

## 1. 规格驱动（Spec-Driven）

- 唯一规格目录：项目根目录下的 [`spec/`](./spec/)，包含三份文档：
  - [`spec/spec.md`](./spec/spec.md) —— 需求与设计规格（Why / 架构 / 错误模型 / API 详案 / Requirements）
  - [`spec/tasks.md`](./spec/tasks.md) —— 任务清单与依赖关系；**任务完成后必须勾选 `[x]`**
  - [`spec/checklist.md`](./spec/checklist.md) —— 验收检查清单；**验证通过后必须勾选 `[x]`**
- 所有后续 task 都要遵循 spec 的规定；实现不得偏离规格。
- 实现过程中发现规格有误或不完整：先修订 `spec.md`（说明理由），经确认后再改代码。**规格与代码冲突时，以最新修订的 spec 为准。**
- 并行求值（`Parallel(n)`）原为 spec 后续 TODO，已随用户确认的 goal 立项并实现（Task 8，语义见 spec「并行求值 v1」）。

## 2. 项目专属约定

- Git 提交规范、Shell 执行环境、语言与文档、标准流程、环境中立等**通用规则**见 [`agents/common.md`](./agents/common.md)；本节仅补充项目专属内容：
  - commit `scope` 常用值：stream / pipeline / splitterator / collector / spec / docs 等
  - 依赖最小化：v1 不引入第三方运行时依赖（测试工具除外）
  - 不过度设计：不实现 spec 未要求的功能（Tier C 明确不做清单见 spec）
  - 文件组织遵循 spec「Impact」一节的文件布局；新增文件需在 spec 中补记
- Go 语言规范（工具链、泛型方法约束、错误模型、比较器签名、现代写法清单）与质量门槛命令见 [`agents/go.md`](./agents/go.md)。

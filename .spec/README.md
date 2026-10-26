# Spec 索引与全局约定

> 对应规范：[AGENTS.md](../AGENTS.md)「规格文档」与 [.agents/general.md](../.agents/general.md) 第 1 节。
>
> 执行循环（spec 驱动）：讨论（先过需求收敛门禁：核心对象 / In-Out Scope / 成功信号 / 关键未知，见 [.agents/general.md](../.agents/general.md) 第 1 节）→ 写入 `.spec/tasks.md` → 确认 → 修改代码 → checklist 验收 → 勾选 tasks。行为类变更 SHALL 同批携带 spec 对应文件改动，使语义分歧在合入前暴露；外部需求文档不得直接作为实现依据，须先做冲突检查（含 `decisions.md` 中被否决/已取代方案，防复活）。
>
> 编写格式：`Requirement`（SHALL 条款）+ `Scenario`（WHEN/THEN，落到可核对的行为）+ **决策依据**（当前口径的关键取舍原因，后续修改前先审视，避免反复）。决策史（背景、备选方案与否决理由、后果）按日期记录于 [decisions.md](decisions.md)，决策依据只留当前理由与 decisions 指针。起草时尚未实证的口径以 `待实证：<触发事件或条件>` 显式标注，实证后转为决策依据或摘除；含标注的条款视为已放行未销账，checklist 验收时逐条核对。实现与 spec 冲突时，先修订对应文件并获用户确认，再实现。

## 项目定位

基于 Go 1.27 **泛型方法**、对标 Java Stream API 的流式处理库（模块 `github.com/JayceChant/go-stream`，go 1.27，根包 `stream`）。泛型方法落地前，`Stream[T]` 上的 `Map[U]` 这类「方法自带类型参数」的链式 API 只能以包级函数表达；本库以原生 Go 提供自然链式调用：`stream.Of(xs...).Filter(p).Map(f).ToSlice()`。零第三方运行时依赖，语句覆盖率 100% 基线。技术约束与质量门槛以 [AGENTS.md](../AGENTS.md) 与 [.agents/go.md](../.agents/go.md) 为准。

## 组织方式（根级单层）

`.spec/` 以点前缀标识规格目录，区别于普通项目文件（默认隐藏，检索时需显式包含，如 `rg --hidden` 或 `grep -r --hidden`）。

当前为**小型单域项目**，规格按「文档类型」组织在 `.spec/` 根级，**不设域子目录**；根级位置即全局作用域，不属于任何域：

- [README.md](README.md) —— 索引与全局约定（组织方式、EARS/ADR 边界、编写格式；非规范，规范条款在 `requirements.md`）
- [requirements.md](requirements.md) —— EARS 行为契约（可观测行为、跨边界契约、业务规则）
- [decisions.md](decisions.md) —— ADR 决策史（背景、备选方案与否决理由、后果；不作为实现依据）
- [tasks.md](tasks.md) —— 任务清单（未勾选 = 待执行的前瞻任务；勾选 = 已执行并通过验收）
- [checklist.md](checklist.md) —— 固定验收动作清单（每次任务收尾须按序核对，不勾选）

**拆分触发条件**：任一规格文件超过 300 行，或出现两个以上相互独立、可并行演进的业务域并导致编辑冲突时，新建 `.spec/<域>/requirements.md`（EARS）与 `.spec/<域>/decisions.md`（仅有决策时创建），并在本文件登记域索引。拆分前不预先创建空目录；拆分后根级文件只保留全局/跨域条目。

### EARS 与 ADR 的边界

- **EARS（`requirements.md`）只写可观测契约**：调用方/用户可观察的行为与反馈（结果内容、顺序、空态、错误与 panic 表现）、跨边界契约（公开 API 名称/签名/参数语义、返回值、特征位声明、import 路径）、业务规则（时间/计数边界、状态流转、短路与降级条件）。判据（**删除测试**）：删掉所有实现名词后，条款仍能被 QA/集成方判定真伪——不能，则它是实现说明，不属于 EARS。库场景例外：公开 API 面与签名本身就是跨边界契约，可以保留；内部类型、函数、文件组织不得进入条款。
- **ADR（`decisions.md`）承接实现选择**：架构与算法、内部组件与函数复用、文件与模块组织、工具链与构建配置、性能取舍。ADR 记录决策时背景、备选方案与否决理由、后果，**不作为实现依据**，不重复行为条款。机制细节可指向 [docs/design.md](../docs/design.md) 与 [docs/api.md](../docs/api.md)。
- **互斥与互补**：requirements 的「决策依据」只留一句当前理由 + `decisions.md` 指针；decisions 的「关联」指回 Requirement 标题或实现位置。`requirements.md` 是唯一规范源；它与 `docs/api.md` 冲突时以 requirements 为准。
- **何时写 ADR（清单制）**：①有真实备选且取舍非显然；②用户裁决过需求盲区；③跨领域或基础性选择（选型、目录结构、治理机制、CI 结构）；④回退成本高；⑤被否决方案值得后人知道（防复活）。普通功能不写 ADR。已采纳条目的方向变化写新条目并标注取代关系，口径微调追加「修订记录」，不得静默改写既有结论。

### 登记与引用规则

- `requirements.md` 条目按**重要性**排列（非加入时间）：①被多处依赖的基础能力 → ②主流程/核心 API → ③辅助能力与局部优化，同级按影响面排序；新增条目按重要性插入对应位置，不追加末尾。
- `decisions.md` 新条目置顶（日期倒序）；ADR 标题与 Requirement 标题均为全仓引用键（引用形态 `.spec/requirements.md「标题」` 或 `.spec/decisions.md「标题」`），修订不得改标题。
- 精简动作（裁剪、合并、转移）随当次任务提交，不必单独立任务；决策史不删除，由 git 历史承载执行细节。

## 文件索引

| 文件 | 内容 |
|---|---|
| [requirements.md](requirements.md) | EARS：构造/中间/终止、Collector、Splitterator、错误模型、NumberStream、并行、生命周期、扩展能力、Join、时间窗口、API 形态、示例 |
| [decisions.md](decisions.md) | ADR：目录与规范体系、核心架构与 API 形态、特征位、并行、时间窗口、交付批次 |
| [tasks.md](tasks.md) | 任务清单 |
| [checklist.md](checklist.md) | 固定验收动作 |
| [../docs/design.md](../docs/design.md) | 架构原理（非规范，实现说明） |
| [../docs/api.md](../docs/api.md) | API 速查参考（非规范） |

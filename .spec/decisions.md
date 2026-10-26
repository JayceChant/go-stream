# 决策记录（ADR）

> 决策史（背景、备选方案与否决理由、后果，按日期倒序），**不作为实现依据**；现行行为契约见 [requirements.md](requirements.md)，组织规则见 [README.md](README.md)。
>
> 触发门槛（清单制）：①有真实备选且取舍非显然；②用户裁决过需求盲区；③跨领域或基础性选择（选型、目录结构、治理机制、CI 结构）；④回退成本高；⑤被否决方案值得后人知道（防复活）。普通功能不写 ADR。已采纳条目的方向变化写新条目并标注取代关系；口径微调追加「修订记录」，不得静默改写既有结论。

## 2026-10-26 规格目录迁移：.spec/.agents 与 EARS + ADR 双文件

- **状态**：已采纳（现行规则见 [README.md](README.md) 与 [.agents/general.md](../.agents/general.md) 第 1 节）
- **关联**：全仓文档；`AGENTS.md`、`.agents/general.md`、`.agents/go.md`、`stream.go`、`.github/workflows/ci.yml`；`tasks.md` Task 27
- **背景**：原 `spec/spec.md` 单文件混合行为契约、架构设计、API 详案与文件布局（395 行），历史口径另置 `legacy.md`；需求与实现选择无边界，检索与并行编辑易冲突，决策背景只能靠 git 考古。协作规范同为普通目录 `agents/`，与项目文件不易区分。
- **决策**：
  - 目录点前缀化：`agents/` → `.agents/`（`common.md` 更名 `general.md`）、`spec/` → `.spec/`。
  - `.spec/` 根级五文件：README（索引/约定，非规范）、requirements（EARS 可观测契约）、decisions（ADR 决策史）、tasks、checklist；小型单域项目不设域子目录，根级即全局作用域。
  - ADR 取代 `legacy.md`：决策史记录背景/备选/后果，按日期倒序；不再保留无格式降级档案。
  - 每规格文件目标 ≤300 行；拆分触发条件写入 README。
- **备选方案与否决理由**：
  - 保留单文件、只加 ADR——需求与实现仍混写，删除测试无从执行，否决；
  - 类型优先目录（`ears/` + `adr/`）——同域两层分居两树、易只读一半，否决；
  - 现在即建域子目录——单域小项目增加空目录与索引维护成本，且无并行编辑冲突，否决（保留增长触发条件）；
  - 保留 legacy.md——与 ADR 双写易漂移、决策背景仍缺备选与后果，否决。
- **后果**：全仓引用形态改为 `.spec/requirements.md「标题」` / `.spec/decisions.md「标题」`；新增/修订 Requirement 须过删除测试；实现机制细节指向 `docs/design.md` / `docs/api.md`；`requirements.md` 为唯一规范源。
- **开源纪律**：文档与提交不得出现内部项目名、内部仓库路径或内部服务名。

## 2026-10-03 时间窗口重采样：UTC 桶键、不产空桶与顺序策略

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「时间窗口重采样」）
- **关联**：`b80e8e4`、`edfe5c2`..`24ddd39`；`time_window.go`、`time_window_test.go`、`time_window_example_test.go`
- **背景**：固定时间间隔重采样（tumbling window）与已有 `WindowSliding`（逐元素）、`Chunk`（定长）互补；混合时区数据的同一瞬间若以 `time.Time` 直接作 map 键，会因 Location 指针不同被拆成多桶；空桶与填值是两类语义。
- **决策**：桶键 `ts(v).Truncate(d).UTC()`（同一瞬间恒落同一桶，`Start` 为 UTC 网格点）；桶序为首现序、桶内相遇序；不产空桶；不设独立聚合入口（由 `Map` 组合表达）；配套 `SortedByTime`（时间序）与 `CompleteTimeBuckets`（空档补全，范围数据驱动、结构补全与填值分离，`maxTimeBuckets` 上限 `1<<20`）。
- **备选方案与否决理由**：直接以 `time.Time` 作键——混合时区拆桶，否决；默认补空桶——无元素窗口是否存在属调用方域，且会掩盖数据缺失，否决；默认按时间排序——常见采样已时间有序，额外排序无收益，乱序由 `SortedByTime` 显式表达，否决；独立聚合法——聚合形态无限，交由 `Map`/collector 组合，否决。
- **后果**：调用方需对 `Start` 做 `.In(loc)` 展示；乱序上游须显式 `SortedByTime`；空桶填值须后接 `Map`。

## 2026-10-03 特征位语义审计与 SpLimited 有限性声明

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「Splitterator 抽象与特征位」；传播表见当前实现与 [docs/design.md](../docs/design.md) 第 3 节）
- **关联**：`7bbe4c6`、`ded045a`、`02dc2f5`；`spliterator.go`、`pipeline.go`、`op.go`、`ops_stateful.go`、`parallel.go`、`op_ext.go`
- **背景**：特征位对齐 Java StreamOpFlag，置位必须为真，否则下游预分配、保序与有限性判定会出错；Task 24 引入 Join 需要「已知有限」守卫，原特征位无法表达。
- **决策**：1:1 变换保留 `SpSized`、清 `SpSorted`/`SpDistinct`；子集变换清 `SpSized`；1:N 变换清 `SpSized`/`SpSorted`/`SpDistinct`；置换类（Sorted/StableSorted/DistinctBy/Reverse）互不清对方位，Reverse 保留 `SpSorted`；物化型算子置 `SpSized`+`SpSubSized`。`SpLimited`：`SpSized ⇒ SpLimited`，FromMap 置位，FromFunc/FromSeq/FromChannel 与无限源不置位，物化类强制置位，Concat/Join/LeftJoin 双侧 AND，Zip 取短 OR；Concat 的 `SpOrdered` 双侧 AND，其余特征位按位或。
- **备选方案与否决理由**：照搬 Java 表——其 SORTED 绑定比较器语义与本库不同，且缺少有限性位，否决；Join 运行期检测无限右流——可能挂死，否决（改为链接期 panic）。
- **后果**：新增算子须同步维护特征位；Join/LeftJoin 右流必须已知有限；Fuzz 覆盖分片并集与并行等价。

## 2026-10-03 协作机制沉淀：AGENTS 历史注记与固定验收清单

- **状态**：已采纳（现行规则见 [.agents/general.md](../.agents/general.md) 与 [checklist.md](checklist.md)）
- **关联**：`AGENTS.md`、`checklist.md`、`tasks.md`
- **背景**：`AGENTS.md` 曾混入「并行求值原为 spec 后续 TODO」等历史注记；checklist 曾按任务逐条勾选，导致清单膨胀、验收动作不固定。
- **决策**：历史注记沉降为 ADR/决策记录；checklist 改为固定有序验收动作（不勾选），随项目实际动态调整；勾选纪律统一为「先勾选 tasks、后提交」。
- **备选方案与否决理由**：保留逐任务勾选——清单不可维护，否决；验收动作散落各 task——易遗漏，否决。
- **后果**：新增验收动作先更新 checklist；历史决策不再写入 AGENTS 正文。

## 2026-09-12 API 形态原则：方法优先、Join 方法化与 deprecated adapter

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「API 形态与兼容性」「双流条件连接 Join」）
- **关联**：`fbb0c73`、`ca85ade`；`construct.go`、`lifecycle.go`、`op_ext.go`
- **背景**：Go 1.27 泛型方法硬限制使部分 API 只能包级；早期 `Concat`/`Cache` 为包级，与链式风格不一致；Join 原定「包级 Join + 方法 LeftJoin」，双流风格不统一；直接删除旧签名会破坏下游。
- **决策**：仅受硬限制的 API 用包级函数；`Concat`/`Cache` 迁为方法，旧包级签名保留为 deprecated adapter（一行委托 + godoc `Deprecated` + `//go:fix inline`），下一版本移除并标 BREAKING；Join/LeftJoin 统一方法形态；仓内调用点全部迁移。
- **备选方案与否决理由**：保留双形态不 deprecated——API 面长期分叉，否决；本版本直接删除——破坏下游且无迁移窗口，否决；Join 保留包级——与 LeftJoin 形态不一致，否决；反射/断言模拟方法约束——牺牲类型安全，否决。
- **后果**：新增 API 先评估方法形态；deprecated 函数只保留一个版本；下游需按 godoc 迁移。

## 2026-09-07 OfNonZero 命名

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「Stream 构造」）
- **关联**：`6418562`
- **背景**：原 `OfNonNil` 只描述引用类型，但函数实际过滤全部零值（数值 0、空串、nil）。
- **决策**：更名 `OfNonZero[T comparable]`；zero ⊇ nil，语义与 `cmp.Or`/`lo.Compact` 术语一致。
- **备选方案与否决理由**：保留 `OfNonNil`——名称窄于行为，误导调用方，否决；拆分 `OfNonNil`/`OfNonZero`——语义重叠，否决。
- **后果**：旧名不再存在；文档与示例统一使用 `OfNonZero`。

## 2026-09-07 流扩展第一批：能力缺口与形态选择

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「流扩展第一批」）
- **关联**：Task 19~23；`terminal.go`、`collector/collector_combine.go`、`ops_stateful.go`、`op_ext.go`、`construct.go`、`collector` 子包
- **背景**：对齐 Java 25 Stream 能力缺口：range-over-func 互通、收集器组合、滑动窗口、单遍统计、闭区间与零值过滤。
- **决策**：`ToSeq()` 以终止求值适配 `iter.Seq`；收集器组合沿用 downstream/teeing 形态；`WindowSliding` 包级（返回 `Stream[[]T]` 触发实例化循环）；`SummaryStats`/`Summarizing` 以单一泛型收集器覆盖数值类型，根包提供 `Summary` 便捷终端；`RangeClosed`/`OfNonZero` 与既有源构造函数并列。
- **备选方案与否决理由**：流上 `iterator()` 双向遍历——单向出站适配已满足 range-over-func，否决；桶/窗口独立聚合入口——聚合形态无限，交由 `Map`/collector 组合，否决；逐类型复制 Summary 收集器——泛型可覆盖，否决。
- **后果**：扩展能力保持组合式；新增收集器需同时更新组合生态文档与 SKILL.md。

## 2026-09-07 NumberStream 数值流：约束收窄形态

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「NumberStream 数值流」）
- **关联**：`0848692`；`number_stream.go`、`numeric.go`
- **背景**：方法不能约束接收者已有类型参数，`Sum`/`Avg`/自然序 `Sorted` 等元素约束 API 原本只能包级，链式体验断裂；Java IntStream 的原始类型特化在 Go 无装箱收益。
- **决策**：引入 `NumberStream[N Number]` 值嵌入 `Stream[N]`，把约束收窄到包装类型自有类型参数位；提供收窄入口、19 个核心方法与 `AsStream` 逃逸出口；一次性语义与 Stream 一致，禁止裸别名。
- **备选方案与否决理由**：继续只提供包级数值函数——无链式体验，否决；每种数值类型各写一套流——泛型可覆盖，否决；接口 + 泛型方法——接口方法不能带类型参数，否决。
- **后果**：每级窄链算子有额外句柄分配（构造约 +35%，求值持平），重构造场景先以 `*Stream` 串联、末步收窄；约束收窄不等于装箱规避。

## 2026-09-04 排序语义拆分：Sorted 与 StableSorted

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「中间操作」）
- **关联**：`6fb98b1`
- **背景**：原 `Sorted` 文档写「稳定排序、对齐 slices.SortFunc」，但 `slices.SortFunc` 本身不稳定，表述自相矛盾。
- **决策**：拆为不稳定 `Sorted(cmp)`（pdqsort，对齐 `slices.SortFunc`）与稳定 `StableSorted(cmp)`（对齐 `slices.SortStableFunc`，等键保持相遇序）；不提供自然序包级 `StableSorted`，`StableSorted(cmp.Compare[T])` 已覆盖。
- **备选方案与否决理由**：只保留稳定排序——默认性能下降；只保留不稳定——等键顺序无契约；增加自然序 StableSorted——API 面冗余；均否决。
- **后果**：调用方按是否需要等键稳定选择；文档明确 `Sorted` 不稳定。

## 2026-09-04 Collector 接口化

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「Collector 汇聚抽象」）
- **关联**：`3f567cd`；`collector/collector.go`
- **背景**：原 Collector 为导出 struct + 函数字段，外部可直接改写字段，破坏行为只读与并行安全；`Combiner` 需要表达「不支持并行」。
- **决策**：`Collector[T,A,R]` 改为接口（Supplier/Accumulator/Combiner/Finisher），各收集器为非导出具体类型；`Combiner` 返回 nil 表示不支持并行，`Collect` 自动降级串行。
- **备选方案与否决理由**：继续导出 struct——字段可被改写，否决；在 struct 上加 unexported 字段——外部无法构造自定义收集器，否决。
- **后果**：自定义收集器实现同一接口；内置收集器行为只读；`BenchmarkCollect` 回测无性能劣化。

## 2026-09-02 示例目录：独立 module 与覆盖率隔离

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「示例目录」）
- **关联**：`262e7b8`；`example/go.mod`、`.github/workflows/ci.yml`、`sonar-project.properties`
- **背景**：可运行示例目录便于用户整文件复制，但无测试文件的包会被 Go 1.22+ 以 0% 计入 coverprofile，拉低根模块 100% 基线；示例需保持可编译不烂尾。
- **决策**：`example/` 为独立 Go module（`example/go.mod` + `replace` 指向根模块），从根模块 `./...` 隔离；CI 单独 build+vet；SonarCloud `sonar.exclusions` 排除 `example/**`；根包 `example_test.go` 的 Example 函数仍属根模块测试。
- **备选方案与否决理由**：同 module 示例——覆盖率基线被拉低，否决；只在测试内写 Example——代码片段不能整文件运行，否决；不纳入 CI——示例腐烂，否决。
- **后果**：新增示例在 `example/<名称>/` 下建独立 main 包；根模块依赖改动经 `replace` 自动生效。

## 2026-08-29 并行求值 v1：分片、降级与无序流式合并

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「并行求值」「生命周期与可重放」）
- **关联**：`c3476a4`；`parallel.go`、`pipeline.go`、`terminal.go`
- **背景**：Java 用 ForkJoinPool + trySplit；Go 无 raw type，异构 stage 链无法以同型字段保存源，需要类型擦除的分片入口；有状态/双流/短路阶段无法安全并行。
- **决策**：pipeline 携带类型擦除的分片闭包，仅可分源（slice/range）构造时设置；每片 goroutine 独立重入求值，每片全新 sink 链与终端累积；物化型有状态、单遍有状态、双流、短路终端族、不可分源自动降级串行；Collect 走片级 Supplier/Accumulator + 按片序 Combiner；无序流先完成先推；片内首错按片序合并，回调 panic 捕获后由发起 goroutine 原样 re-panic。
- **备选方案与否决理由**：共享 sink 链——数据竞争，否决；所有算子都并行——正确性无法保证，否决；引入第三方并行库——违背零依赖，否决。
- **后果**：`go test -race` 必跑；CPU 密集场景实测约 3.3x（4 分片）；新增有状态算子须声明并行降级。

## 2026-08-29 求值引擎：闭包组合与单遍融合

- **状态**：已采纳（机制详见 [docs/design.md](../docs/design.md) 第 2~3 节）
- **关联**：`stream.go`、`pipeline.go`、`op.go`、`sink.go`
- **背景**：Java 以链表 stage + 模板方法组织求值；Go 无 raw type，`Map` 前后元素类型不同，异构上游无法存入同型字段；接口 + 泛型方法受硬限制。
- **决策**：每个 stage 在构造期捕获上游求值闭包，求值时先包装下游 sink、再驱动上游；无状态算子单遍融合，有状态算子按需物化上游段；短路由 `Accept` 返回 bool 表达；终止操作触发一次求值并消费流。
- **备选方案与否决理由**：接口 stage 链——接口方法不能带类型参数，否决；运行期类型擦除 + 断言——失去编译期类型安全，否决；所有算子均物化——无谓分配，否决。
- **后果**：新增无状态算子写 wrap 闭包、有状态算子声明物化策略；机制说明以 `docs/design.md` 为准。

## 2026-08-29 生命周期与可重放：OnClose/Close 与 Cache

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「生命周期与可重放」）
- **关联**：`1331adc`；`lifecycle.go`
- **背景**：流可能持有 channel/文件等资源；一次性模型下重复求值会 panic，但调用方常需多次遍历同一批数据。
- **决策**：`OnClose` 注册清理回调，终止求值结束时自动触发一次，`Close` 幂等显式关闭；`Cache()` 首次调用物化上游一次，此后返回独立一次性流；`Unordered()` 允许并行分片先完成先推，结果集合与串行一致。
- **备选方案与否决理由**：依赖 runtime finalizer 释放——时机不可控，否决；Cache 共享同一个流——违反一次性模型，否决；Cache 每次重新求值——失去重放意义，否决。
- **后果**：资源型源应配对 `OnClose`；Cache 物化期错误由后续流携带；无序流不保证顺序。

## 2026-08-29 特征位语义修正：Map/MapErr 保留 SpSized

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「Splitterator 抽象与特征位」）
- **关联**：Task 6；`ops_stateless.go`、`op.go`
- **背景**：原规则 Map/MapErr 清 `SpSized`，导致下游无法按 size 预分配，1e2 规模开销 4.8x 超标。
- **决策**：1:1 变换保留 `SpSized`，仅清 `SpSorted`/`SpDistinct`；修正后同场景开销降至 2.84x。
- **备选方案与否决理由**：维持清 `SpSized`——预分配机会丢失，否决；按元素数变化动态判断——1:1 变换数量恒等，无必要，否决。
- **后果**：性能基准纳入质量门槛；后续特征位规则见「特征位语义审计与 SpLimited 有限性声明」。

## 2026-08-28 立项与 Java Stream → Go 设计取舍（v1 → v2）

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md) 各节）
- **关联**：`510c133`；`README.md`、`docs/design.md`
- **背景**：Go 1.27 泛型方法落地前，`Stream[T].Map[U]` 这类链式 API 只能用包级函数；目标是以原生 Go 提供 Java Stream 风格的自然链式调用。初版方案经用户反馈修订为 v2（模块路径、并行延后、API 详案、错误即值、文档要求、组合替代继承）。
- **决策**：API 以具体 struct `*Stream[T]` + 泛型方法公开，接口仅用于 `Sink`/`Splitterator`/`Collector`；数据源用 Go 原生 slice/map/channel/`iter.Seq`/生成器；不做原始类型特化流（Go 泛型零装箱），约束收窄由 `NumberStream` 承接；比较器统一 `func(a,b T) int`；错误即值；生命周期用 `OnClose`/`Close`；有状态算子直接物化 `[]T`，不建 Node 树；明确非目标：错误化普通回调/Collector Finisher、库层限速/背压、双向 iterator。
- **备选方案与否决理由**：接口形态 Stream——接口方法不能带类型参数，无法链式泛型，否决；原始类型特化——Go 零装箱，收益不足，否决；引入第三方依赖——违背零依赖，否决。
- **后果**：所有公开 API 围绕 `*Stream[T]`；`docs/design.md` 记录 Java→Go 映射。

## 2026-08-28 Go 1.27 泛型方法约束与组合替代继承

- **状态**：已采纳（现行规则见 [.agents/go.md](../.agents/go.md) 第 2 节；机制见 [docs/design.md](../docs/design.md) 第 1~2 节）
- **关联**：`stream.go`、`pipeline.go`、`op.go`
- **背景**：Java Stream 骨架是单继承类树（模板方法 + 子类覆写）；Go 不支持继承，且泛型方法有三条硬限制（已实测 go1.27.0）：①接口方法不得声明类型参数；②方法不得约束接收者已有类型参数；③方法返回 `T` 的派生类型触发实例化循环。
- **决策**：`Stream[T]` 为具体泛型 struct，嵌入非导出 `pipeline[T]`；异构 stage 以构造期捕获的求值闭包链接；算子以构造函数 + wrap 闭包实现，不用「嵌入 + 期望覆写」模拟继承；需约束 `T` 的 API 用包级函数，数值约束由 `NumberStream` 包装类型承接方法形态；需返回 `T` 派生类型的 API（Chunk/Enumerate/WindowSliding/TimeWindow）用包级函数。判定口诀：新增类型参数随便加；要动 `T` 本身（约束或派生返回）就降级包级。
- **备选方案与否决理由**：接口 + 泛型方法——规则①禁止，否决；代码生成多份特化——维护成本高，否决；反射/`any` 擦除——失去编译期类型安全，否决。
- **后果**：API 形态受硬限制；新增 API 先按口诀判定；`docs/design.md` 记录映射。

## 2026-08-28 错误即值模型

- **状态**：已采纳（现行契约见 [requirements.md](requirements.md)「错误即值模型」；机制见 [docs/design.md](../docs/design.md) 第 5 节）
- **关联**：`sources.go`、`ops_stateless.go`、`terminal.go`、`pipeline.go`
- **背景**：Java 以 unchecked 异常穿透；Go 惯例是 error 值。流式回调若全部带 error 签名会让纯变换场景充满错误噪音；拉式 IO 源又必须表达失败。
- **决策**：可预期且可恢复的错误以 error 值传播——`FromFunc` 源、四个高频 Err 变体（MapErr/FilterErr/FlatMapErr/PeekErr）、首错短路、部分结果保留、`Err()` 查询；不可恢复的编程错误（重复消费、nil 回调）panic；用户回调 panic 原样传播。普通算子回调不带 error 签名；Collector 的 Accumulator/Finisher 不引入错误签名。
- **备选方案与否决理由**：所有回调带 error——纯变换场景噪音过大，否决；异常式 panic 传播可预期错误——调用方无法恢复，否决；Collector 错误化——破坏组合简洁性，否决。
- **后果**：fallible 汇聚用「先 Collect 到中间容器再校验」表达；错误模型以 Scanner/Rows 惯例为参照。

## 2026-08-28 包结构与文件布局

- **状态**：已采纳（文件布局见 [AGENTS.md](../AGENTS.md) 第 2.1 节）
- **关联**：Task 9 `6fb4b8d`、Task 16 `7498c92`、Task 17 `3f567cd`；`constraints/`、`collector/`、根包
- **背景**：根包引擎与算子群存在非导出符号循环互访与字段级裸访问；强行拆分需导出内部符号或整体下沉 `internal`，收益低于成本。数值约束被 collector 子包复用；Collector 与引擎低耦合。
- **决策**：`constraints` 为零依赖叶子包（Integer/Float/Number），根包以类型别名保留公开形态；`collector` 子包承载 Collector 接口与预置/组合收集器，对根包零非导出依赖、零引擎依赖；根包一体承载引擎与算子群；测试文件与被测代码同目录，示例独立 module。
- **备选方案与否决理由**：引擎拆 `internal`——需整体重做公共接口，否决；collector 并入根包——子包复用与低耦合优势丢失，否决；constraints 并入根包——collector 反向依赖根包，否决。
- **后果**：新增算子放根包对应文件；collector 新增收集器放子包；跨包只依赖公开 API。

## 2026-08-28～09-12 Task 1～23 交付时间线（档案）

- **状态**：历史档案（不作为实现依据；执行细节见 git 历史）
- **关联**：`tasks.md`
- **说明**：Task 1~7（08-28~29）脚手架与核心类型 / 求值引擎 / 构造函数与 Splitterator / 中间操作含 Err 变体 / 终止操作与 Collector / 端到端测试与基准 / Markdown 文档；Task 8（08-29 `c3476a4`）并行求值；Task 9（08-29 `6fb4b8d`）collector 子包；Task 10（08-29 `1331adc`）OnClose/Close、Cache、Unordered；Task 11（08-29 `ebd5fc5`）白盒补缺 + Fuzz，修复 3 个真实 bug；Task 12（09-02 `a9395bb`）drive 链命名；Task 13（09-02 `fca8f3e`）覆盖率 100%；Task 14（09-02 `c969a97`）Skip(0) 恒等；Task 15（09-02 `262e7b8`）example 目录；Task 16（09-04 `7498c92`）数值约束下沉；Task 17（09-04 `3f567cd`）Collector 接口化；Task 18（09-05 `0848692`）NumberStream；Task 19~23（09-07）ToSeq / Collector 组合 / WindowSliding / Summary / RangeClosed+OfNonZero；Task 24~26（09-12~10-03）Join / 方法化 / TimeWindow。
- **决策史入口**：各决策见本文件对应日期的 ADR。

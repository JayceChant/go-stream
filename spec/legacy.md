# Legacy（降级档案）

> 按时间序追加的被取代/已关闭决策记录，无格式要求，仅作考古参考，**不作为实现依据**；现行口径一律以同目录 `spec.md` 为准。条目在口径被取代时移入，避免正文膨胀。

- 2026-08-28 spec v1 立项（`510c133`）→ 按用户反馈修订为 v2：模块路径 / 并行延后 / API 详案展开 / 错误即值 / 文档要求 / 组合替代继承。
- 2026-08-29 阶段划分：并行求值原为「阶段 1 后续 TODO、接口层预留」（`c3476a4` 已随 Task 8 交付）；「并行接口预留」Requirement 整段被「并行求值 v1」取代。
- 2026-08-29 Map/MapErr 特征位由「清 SpSized」改为「保留 SpSized、仅清 Sorted/Distinct」（Task 6 修订）：原规则致下游无法按 size 预分配，1e2 规模开销 4.8x 超标，修正后 2.84x。〔被「Splitterator 抽象与特征位」吸收〕
- 2026-09-04 `Sorted` 原表述「稳定排序、对齐 slices.SortFunc」自相矛盾（SortFunc 本身不稳定），拆分为不稳定 `Sorted` + 稳定 `StableSorted`（`6fb98b1`）。
- 2026-09-04 Collector 由 struct（导出函数字段）改为接口 + 非导出具体类型实现（Task 17，`3f567cd`）；BenchmarkCollect 回测无劣化。〔被「Collector 汇聚抽象」取代〕
- 2026-09-07 `OfNonNil` 更名 `OfNonZero`（Task 23，`6418562`）：zero ⊇ nil，函数过滤的是全部零值；对齐 `cmp.Or`/`lo.Compact` 术语。
- 2026-09-12 Join 原定「包级 Join + 方法 LeftJoin」随用户反馈统一方法化（`fbb0c73`）；项目形态原则随之确立：仅受 Go 1.27 泛型方法硬限制的 API 才用包级函数。〔被「双流条件连接 Join」「形态原则与方法化迁移」取代〕
- 2026-09-12~14 `Concat`/`Cache` 方法化，旧包级签名转 deprecated adapter + `//go:fix inline`（Task 25，`ca85ade`）；本版本仅 deprecated，下一版本移除并标 BREAKING。
- 2026-10-03 特征位健全性审计（Task 26）：Filter/FilterErr 补清 SpSized（`7bbe4c6`）；Limit 透传 SpSorted、Sorted/DistinctBy 互不清对方位（`ded045a`，对齐 Java PRESERVE）；Zip SpLimited 改取短 OR（`02dc2f5`）；Reverse 保留 SpSorted（用户决议，「存在比较器」语义）。
- 2026-10-03 TimeWindow 评审决议集：裁撤 TimeWindowBy 独立聚合入口（桶级聚合由 Map 组合表达）；桶键 `.UTC()` 规范化（`b80e8e4`，混合时区等值瞬间不拆桶）；空桶不默认补全（`CompleteTimeBuckets` 显式表达）；桶序维持首现序不作默认时间排序（`SortedByTime` 配套）。
- 2026-10-03 AGENTS.md 历史注记沉降：「并行求值原为 spec 后续 TODO」条目；checklist.md 逐任务勾选记录整体转固定动作清单。〔机制见 agents/common.md 1.1〕

## Task 1–23 已交付批次时间线（详见 git）

- Task 1~7（2026-08-28~29）：脚手架与核心类型 / 求值引擎 / 构造函数与 Splitterator / 中间操作含 Err 变体 / 终止操作与 Collector / 端到端测试与基准 / Markdown 文档。
- Task 8（08-29 `c3476a4`）：并行求值 Parallel(n)/Sequential()，4 分片实测加速比 ~3.3x。
- Task 9（08-29 `6fb4b8d`）：collector 低耦合子包划分。
- Task 10（08-29 `1331adc`）：OnClose/Close、Cache 可重放工厂、Unordered 流式合并。
- Task 11（08-29 `ebd5fc5`）：白盒补缺 26 项 + 7 个 Fuzz 目标，修复 3 个真实 bug（splitSrc 深递归丢元素、sliceTotal 回放短路仅断本片、newRangeSp 漏置 SpSubSized）。
- Task 12（09-02 `a9395bb`）：drive 链命名可读性优化（driveFunc 命名类型、局部变量弃缩写）。
- Task 13（09-02 `fca8f3e`）：语句覆盖率提升至 100%（根包与 collector 子包）。
- Task 14（09-02 `c969a97`）：Skip(0) 恒等返回原流（免物化/保短路/不降并行）。
- Task 15（09-02 `262e7b8`）：example/ 完整可运行示例目录（嵌套独立 module 隔离覆盖率）。
- Task 16（09-04 `7498c92`）：数值约束下沉 constraints 叶子包，Summing 迁入 collector。
- Task 17（09-04 `3f567cd`）：Collector 接口化。
- Task 18（09-05 `0848692`）：NumberStream 数值流（约束收窄形态，19 核心方法）。
- Task 19~23（09-07）：ToSeq 出站适配 / Collector 组合生态 / WindowSliding / Summary 单遍统计 / RangeClosed+OfNonZero。

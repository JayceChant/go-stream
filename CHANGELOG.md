# Changelog

All notable changes to this project will be documented in this file.
由 [release-please](https://github.com/googleapis/release-please) 基于中文 Conventional Commits 自动维护；v0.1.0 及之前为人工回填。

## [0.2.0](https://github.com/JayceChant/go-stream/compare/v0.1.0...v0.2.0) (2026-09-08)

本版本聚焦一次排序语义拆分与两块能力扩展：`Sorted` 改为不稳定 pdqsort（对齐 `slices.SortFunc`，默认更快），稳定语义由新增的 `StableSorted` 承接；新增 `NumberStream` 数值流与配套数值收集器（`Summing`/`Averaging`/`Summarizing` 等）；落地流扩展第一批（`ToSeq`、`WindowSliding`、Collector 组合生态、`RangeClosed`/`OfNonZero`，对齐 Java 25 能力缺口）。工程上接入 release-please 发布自动化，CHANGELOG 与 GitHub Release 自本版起由 Conventional Commits 自动维护。

### Features

* **collector:** 实现 Collector 组合生态（Task 20） ([63d0588](https://github.com/JayceChant/go-stream/commit/63d05881f7f88918da1aa6b8937b6e8f126be5c0))
* **collector:** 实现 Summarizing/SummaryStats 单遍统计（Task 22） ([3650a64](https://github.com/JayceChant/go-stream/commit/3650a6496107b620c3d12035761f43c8038c188b))
* **collector:** 新增 Averaging 数值平均收集器 ([08f90cf](https://github.com/JayceChant/go-stream/commit/08f90cf8967f5f384bbc54df1e4bde456cd6629b))
* **stream:** 合入 NumberStream 数值流特性分支 ([96d33dd](https://github.com/JayceChant/go-stream/commit/96d33dd6db4d22c203d1fe3cf4fd87a8899a911b))
* **stream:** 实现 RangeClosed / OfNonNil 便捷源（Task 23） ([9803738](https://github.com/JayceChant/go-stream/commit/9803738f3080108f323baec8f4b47b4f33123126))
* **stream:** 实现 ToSeq 出站迭代适配（Task 19） ([35fb7a6](https://github.com/JayceChant/go-stream/commit/35fb7a66c38aaadac68fb00e27dd555dcaa69a15))
* **stream:** 实现 WindowSliding 滑动窗口算子（Task 21） ([89bd0d6](https://github.com/JayceChant/go-stream/commit/89bd0d6c5d92214376878dd66914c3c38c0bb15f))
* **stream:** 排序拆分为不稳定 Sorted 与稳定 StableSorted ([6fb98b1](https://github.com/JayceChant/go-stream/commit/6fb98b15390dd8da489957db911c0f3580ca10d9))
* **stream:** 新增 NumberStream 数值流，元素约束 API 回归方法形态 ([0848692](https://github.com/JayceChant/go-stream/commit/08486928c30fdd489f4576da944399b23dc8ffdf))


### Bug Fixes

* **ci:** tools 改为 linked，修正裸版本号不被支持的问题 ([6a67cf3](https://github.com/JayceChant/go-stream/commit/6a67cf313d482c2e487fba9b0277289cc66bfe16))


### Performance

* **stream:** Sorted/Reverse 就地变换独占物化缓冲，省一次全量克隆 ([aaf9f36](https://github.com/JayceChant/go-stream/commit/aaf9f362c1acd6aa0dc6783a5e4baf40dec16e6d))


### Documentation

* **agents:** merge 统一改用 --no-ff -m，规避无编辑器终端阻塞 ([0f46ef2](https://github.com/JayceChant/go-stream/commit/0f46ef2ff7c7a97dade730702eda6c5f7b316890))
* **agents:** 协作规范拆分为通用/Go 语言/项目专属三部分，便于跨项目复用 ([63809bf](https://github.com/JayceChant/go-stream/commit/63809bfea4c1b4c038ffd5239da4ac6951ccee28))
* **collector:** 子包依赖改为模糊表述，防文档随实现漂移 ([f217d8f](https://github.com/JayceChant/go-stream/commit/f217d8fe1ecee48f201bbb9e88ebe016a8bd7749))
* **example:** 新增 extensions 示例（流扩展第一批五特性） ([82dcd36](https://github.com/JayceChant/go-stream/commit/82dcd36cf3fea287c22539ccb55d25e68bc13d67))
* **readme:** roadmap 勾选 v0.2.0 发布条目并补充交付内容 ([ad9268c](https://github.com/JayceChant/go-stream/commit/ad9268c42c6218690d9775252b05b2978600ce59))
* **readme:** 两份 README 补充流扩展第一批新特性信息 ([e58dbf0](https://github.com/JayceChant/go-stream/commit/e58dbf008a42636dbdb7164af1300dd86550fa0e))
* **readme:** 中英双语 README 与数值示例同步 NumberStream ([780c470](https://github.com/JayceChant/go-stream/commit/780c470996cf3bf09970ef25494c0261b8d703b9))
* **readme:** 性能小节改用公平对比口径，Top-K 实测 1.2–1.5x ([06128a2](https://github.com/JayceChant/go-stream/commit/06128a298a12d47c1525b694b8bdeb10d690701d))
* **readme:** 新增「实现对比」章节，风格与性能对照成节 ([113c30e](https://github.com/JayceChant/go-stream/commit/113c30e40303689b372f3f023e1aab8804eef57f))
* **readme:** 路线图按 v0.x 阶段重划分，声明无兼容性承诺 ([7f42258](https://github.com/JayceChant/go-stream/commit/7f4225867669ded06ceac37f903a61a08b5d0d24))
* **spec:** 流扩展第一批收尾——文档同步与勾选（Task 19~23） ([8b4c8ad](https://github.com/JayceChant/go-stream/commit/8b4c8adb72bc836d36266de397ac5c79ba656198))
* **spec:** 立项 Task 19~23 流扩展第一批（对齐 Java 25 能力缺口） ([b3e7937](https://github.com/JayceChant/go-stream/commit/b3e7937ed784d0630476aeadd5dc0116557792d7))
* **stream:** 补记并存 API 形态间的取舍与互引 ([96f1eef](https://github.com/JayceChant/go-stream/commit/96f1eef08ee8e9be2aa426d3fd7f754eea63ebfd))
* **stream:** 补记并存 API 形态间的取舍与互引 2 ([5a81c6b](https://github.com/JayceChant/go-stream/commit/5a81c6bcf91ae5016b0da94947809c2e42ad5cf7))

## 0.1.0 (2026-09-03)

首个发布 / First release：v1 串行求值引擎、全量算子、Collector 体系、错误即值模型、并行求值（`Parallel(n)`），详见 [README_CN.md](./README_CN.md)。

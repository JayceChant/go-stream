# AGENTS.md — go-stream 项目协作规范

> 本文件是**每一次开发循环**都必须遵守的全局规则，适用于所有参与者（人类与 AI agent）。
> 开始任何工作前，先完整阅读本文件。

## 1. 规格驱动（Spec-Driven）

- 唯一规格目录：项目根目录下的 [`spec/`](./spec/)，包含三份文档：
  - [`spec/spec.md`](./spec/spec.md) —— 需求与设计规格（Why / 架构 / 错误模型 / API 详案 / Requirements）
  - [`spec/tasks.md`](./spec/tasks.md) —— 任务清单与依赖关系；**任务完成后必须勾选 `[x]`**
  - [`spec/checklist.md`](./spec/checklist.md) —— 验收检查清单；**验证通过后必须勾选 `[x]`**
- 所有后续 task 都要遵循 spec 的规定；实现不得偏离规格。
- 实现过程中发现规格有误或不完整：先修订 `spec.md`（说明理由），经确认后再改代码。**规格与代码冲突时，以最新修订的 spec 为准。**
- 并行求值（`Parallel(n)`）原为 spec 后续 TODO，已随用户确认的 goal 立项并实现（Task 8，语义见 spec「并行求值 v1」）。

## 2. Git 提交规范（强制）

- **每次被用户接受的修改，必须立即做一次 git commit**，不得积压多任务后混合提交。
- message 采用**中文 Conventional Commits** 格式：

  ```
  <type>(<scope>): <中文简述>

  <正文：动机与要点，可选>
  ```

  - `type`：feat / fix / docs / style / refactor / test / chore / perf / build / ci
  - `scope` 可选，常用：stream / pipeline / splitterator / collector / spec / docs 等
  - 示例：`feat(stream): 实现 Map/Filter 无状态算子`、`docs(spec): 修订错误处理模型`
- 一次提交只做一件事；只 `git add` 与本任务相关的文件，**不得** 将用户未提交的无关改动混入。
- 中文 message：按第 3 节在 Git Bash 中执行时可直接 `git commit -m "中文"`（UTF-8 全链路，已实测验证）；**回退**到 PowerShell/cmd 时，必须将 message 写入 **UTF-8 文件**并用 `git commit -F <file>` 提交，提交后用 `git log` 验证无乱码。临时 message 文件用后即删。
- 遵守常规 git 安全约定：禁止 force push、禁止未授权的历史改写（`reset --hard`/`checkout .`/`restore` 等）、禁止提交含密钥的文件。

## 3. 命令执行环境（Shell 优先级，强制）

所有命令行操作（git、go 工具链、脚本等）统一遵守以下执行环境规则，以提高兼容性（中文/UTF-8/POSIX 工具链）：

- **本地环境路径等配置统一存放于项目根目录的 `.env.local`**（见第 8 节），执行命令前先读取，避免每次重复探测。
- **Linux / macOS**：无需特殊处理，使用默认 shell 直接执行。
- **Windows**：按以下优先级选择执行环境，仅当高优先级不可用时才降级：
  1. **WSL**（首选）：Linux 原生 bash 与 git（UTF-8 全链路，中文安全）；go 工具链经 interop 调用 Windows 侧 `go.exe`（版本需 ≥ go 1.27）。项目路径形如 `/mnt/<盘符>/...`。
     - 注意：`C:\windows\system32\bash.exe` 即 WSL bash；勿与 Git Bash 混淆。
  2. **Git Bash**（次选）：完整 POSIX 工具链，中文安全。项目路径形如 `/d/...`。
  3. **cmd / PowerShell**（末选，仅当上述两者均不支持时）：例如需要 PowerShell 专属 cmdlet / `.ps1` 脚本、Windows 特有工具、COM/WMI 交互等场景。此时警惕中文编码问题（控制台默认 GBK 代码页），涉及中文输出/参数的操作参考第 2 节的文件中转方案。

## 4. 语言与文档

- 所有文档（spec / README / docs）与代码注释使用**中文**。
- 代码标识符（类型、函数、变量）使用英文，遵循 Go 官方命名惯例（驼峰、导出用大写起始、不滥用下划线）。
- 新增或变更公开 API，必须同步更新中文 godoc；涉及用户可见行为时同步更新 `README.md` 与 `docs/api.md`。

## 5. Go 语言规范

- 工具链 **go 1.27**；`go.mod` 声明 `go 1.27`。
- 积极使用**泛型方法**（方法级类型参数，如 `Map[U any]`），但牢记约束：**接口方法不得声明类型参数**，因此对外 API 挂在具体类型 `*Stream[T]` 上。
- **组合优先**：禁止用"嵌入 + 期望子类覆写方法"模拟继承；待定制行为一律以函数值参数注入。
- **错误即值**（详见 `spec/spec.md` 错误处理设计）：
  - 可预期且可恢复的错误 → `error` 值传播（`FromFunc` 源、`MapErr` 等变体、`Err()` 查询）
  - 编程 bug / 不可恢复（重复消费流、nil 回调）→ `panic`，信息清晰
  - 普通算子回调不带 error 签名
- 比较器统一 `func(a, b T) int`（对齐标准库 `slices.SortFunc` 惯例）。
- **紧跟工具链的新写法**：`go fix` 的现代化改写一经采纳即为本项目写法约定，新代码直接采用：
  - 整数计数循环用 `for i := range n`，不用 `for i := 0; i < n; i++`；
  - 并发原子操作用 `sync/atomic` 类型化原子（如 `atomic.Int32` 的 `Add`/`Load` 方法），不用包级函数；
  - 复合字面量初始化嵌入结构体时，同名字段直接写、不再冗余限定内嵌类型名（`gofmt`/`go fix` 会自动消除）。
- 依赖最小化：v1 不引入第三方运行时依赖（测试工具除外）。

## 6. 质量门槛（每次提交前必须全部通过）

```bash
gofmt -l .            # 输出必须为空
go vet ./...          # 无告警
go test ./...         # 全绿
```

- 性能敏感路径的改动需附 benchmark 数据（`go test -bench`），管道额外开销目标 <3x 手写循环。
- 新功能必须带单测；修复 bug 先写复现用例再修复。

## 7. 每次循环的标准流程

1. 读 [`spec/tasks.md`](./spec/tasks.md)，选择下一个未完成任务（遵循 Task Dependencies 顺序）
2. 阅读 spec 中相关章节，实现 + 单元测试
3. 勾选 `tasks.md` / `checklist.md` 对应项
4. 通过第 6 节质量门槛
5. 等待用户接受
6. 按第 2 节规范提交（中文 Conventional Commits）
7. 回到第 1 步

## 8. 环境中立与其它全局规则

- **本地路径不得进入任何提交文档**（AGENTS.md、spec/、README、docs 等）：文档中只允许出现相对项目根目录的路径与本机无关的通用描述。本机绝对路径（shell 位置、项目根、工具链路径等）**只能**写入项目根目录的 `.env.local`。
- **`.env.local` 为本地环境配置 dotfile**：
  - 已在 `.gitignore` 中忽略，**禁止提交**；
  - 键值格式（`KEY=VALUE`），至少包含：`WIN_PROJECT_ROOT`、`WSL_BASH`/`WSL_PROJECT_ROOT`/`WSL_GIT`/`WSL_GO`/`WSL_GOFMT`、`GIT_BASH`/`GIT_BASH_PROJECT_ROOT`；
  - 文件不存在时：按第 3 节优先级**现场探测一次**并生成，后续循环直接读取，避免重复探测。
- **不得回滚用户的手动修改**：工作区可能包含与当前任务无关的用户改动，保持原样、不带入提交。
- 不过度设计：不实现 spec 未要求的功能（Tier C 明确不做清单见 spec）。
- 文件组织遵循 spec「Impact」一节的文件布局；新增文件需在 spec 中补记。
- 提交信息、文档、注释中不得包含无根据的承诺（如未做的优化、未测的性能数据）。

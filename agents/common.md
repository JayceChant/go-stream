# 通用协作规范（语言无关）

> 本文件为**跨项目复用**的语言无关协作规范，由各项目根目录的 `AGENTS.md` 引用生效；开始任何工作前先完整阅读。
> 与具体编程语言相关的规范（命名细节、工具链、质量门槛命令等）见同目录下的语言规范文件（如 [`go.md`](./go.md)）。

## 1. 规格驱动（Spec-Driven）

- 唯一规格目录：项目根目录下的 `spec/`，包含三份文档：
  - `spec/spec.md` —— 需求与设计规格
  - `spec/tasks.md` —— 任务清单与依赖关系；**任务完成后必须勾选 `[x]`**
  - `spec/checklist.md` —— 验收检查清单；**验证通过后必须勾选 `[x]`**
- 所有后续 task 都要遵循 spec 的规定；实现不得偏离规格。
- 实现过程中发现规格有误或不完整：先修订 `spec.md`（说明理由），经确认后再改代码。**规格与代码冲突时，以最新修订的 spec 为准。**
- 项目自身的历史决策与立项记录（如由 spec 后续 TODO 转正的功能）写在各项目 `AGENTS.md` 的项目专属章节，不进入本通用文件。

## 2. Git 提交规范（强制）

- **每次被用户接受的修改，必须立即做一次 git commit**，不得积压多任务后混合提交。
- message 采用**中文 Conventional Commits** 格式：

  ```
  <type>(<scope>): <中文简述>

  <正文：动机与要点，可选>
  ```

  - `type`：feat / fix / docs / style / refactor / test / chore / perf / build / ci
  - `scope` 可选，取项目模块名；常用值见各项目 `AGENTS.md`
  - 示例：`feat(<scope>): 实现 Map/Filter 无状态算子`、`docs(spec): 修订错误处理模型`
- 一次提交只做一件事；只 `git add` 与本任务相关的文件，**不得** 将用户未提交的无关改动混入。
- 中文 message：按第 3 节在 Git Bash 中执行时可直接 `git commit -m "中文"`（UTF-8 全链路，已实测验证）；**回退**到 PowerShell/cmd 时，必须将 message 写入 **UTF-8 文件**并用 `git commit -F <file>` 提交，提交后用 `git log` 验证无乱码。临时 message 文件用后即删。
- `git merge` 统一使用 `--no-ff -m "<message>"`：`--no-ff` 保留分支合并拓扑；`-m` 显式给出 merge message（沿用 `Merge branch '<分支名>'` 风格）。禁止不带 `-m` 的 merge——未配置编辑器的终端（EDITOR/core.editor 缺失）会弹出 vi 编辑 `MERGE_MSG`，阻塞非交互命令执行。
- 遵守常规 git 安全约定：禁止 force push、禁止未授权的历史改写（`reset --hard`/`checkout .`/`restore` 等）、禁止提交含密钥的文件。

## 3. 命令执行环境（Shell 优先级，强制）

所有命令行操作（git、语言工具链、脚本等）统一遵守以下执行环境规则，以提高兼容性（中文/UTF-8/POSIX 工具链）：

- **本地环境路径等配置统一存放于项目根目录的 `.env.local`**（见第 7 节），执行命令前先读取，避免每次重复探测。
- **Linux / macOS**：无需特殊处理，使用默认 shell 直接执行。
- **Windows**：按以下优先级选择执行环境，仅当高优先级不可用时才降级：
  1. **WSL**（首选）：Linux 原生 bash 与 git（UTF-8 全链路，中文安全）；工具链经 interop 调用 Windows 侧可执行文件（如 `go.exe`，版本要求见语言规范文件）。项目路径形如 `/mnt/<盘符>/...`。
     - 注意：`C:\windows\system32\bash.exe` 即 WSL bash；勿与 Git Bash 混淆。
  2. **Git Bash**（次选）：完整 POSIX 工具链，中文安全。项目路径形如 `/d/...`。
  3. **cmd / PowerShell**（末选，仅当上述两者均不支持时）：例如需要 PowerShell 专属 cmdlet / `.ps1` 脚本、Windows 特有工具、COM/WMI 交互等场景。此时警惕中文编码问题（控制台默认 GBK 代码页），涉及中文输出/参数的操作参考第 2 节的文件中转方案。

## 4. 语言与文档

- 所有文档（spec / README / docs）与代码注释使用**中文**。
- 代码标识符（类型、函数、变量）使用英文，遵循所选语言的官方命名惯例（细节见语言规范文件）。
- 新增或变更公开 API，必须同步更新对应文档；涉及用户可见行为时同步更新 `README.md` 与 `docs/api.md`。

## 5. 质量门槛总则（强制）

- **每次代码修改后、提交前，必须完整通过项目约定的质量检查**（具体命令清单见语言规范文件，如 [`go.md`](./go.md) 第 3 节），任一失败不得提交。
- 新功能必须带单测；修复 bug 先写复现用例再修复。

## 6. 每次循环的标准流程

1. 读 `spec/tasks.md`，选择下一个未完成任务（遵循 Task Dependencies 顺序）
2. 阅读 spec 中相关章节，实现 + 单元测试
3. 勾选 `spec/tasks.md` / `spec/checklist.md` 对应项
4. 通过质量门槛（总则见第 5 节，命令清单见语言规范文件）
5. 等待用户接受
6. 按第 2 节 Git 提交规范提交（中文 Conventional Commits）
7. 回到第 1 步

## 7. 环境中立与其它全局规则

- **本地路径不得进入任何提交文档**（AGENTS.md、agents/、spec/、README、docs 等）：文档中只允许出现相对项目根目录的路径与本机无关的通用描述。本机绝对路径（shell 位置、项目根、工具链路径等）**只能**写入项目根目录的 `.env.local`。
- **`.env.local` 为本地环境配置 dotfile**：
  - 已在 `.gitignore` 中忽略，**禁止提交**；
  - 键值格式（`KEY=VALUE`），至少包含：`WIN_PROJECT_ROOT`、`WSL_BASH`/`WSL_PROJECT_ROOT`/`WSL_GIT`、`GIT_BASH`/`GIT_BASH_PROJECT_ROOT`，以及语言工具链路径键（Go 项目为 `WSL_GO`/`WSL_GOFMT`）；
  - 文件不存在时：按第 3 节优先级**现场探测一次**并生成，后续循环直接读取，避免重复探测。
- **不得回滚用户的手动修改**：工作区可能包含与当前任务无关的用户改动，保持原样、不带入提交。
- 不过度设计：不实现 spec 未要求的功能（明确不做清单见各项目 spec）。
- 提交信息、文档、注释中不得包含无根据的承诺（如未做的优化、未测的性能数据）。

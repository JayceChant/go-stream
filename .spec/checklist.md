# Checklist

> 每次任务收尾时**固定执行**的验收动作，按序核对后方可提交；本清单随项目实际动态调整——过时项删除、新有用项加入，不为历史保留条目（不勾选）。

1. 质量门槛：`go fix` / `gofmt -l .` / `go vet ./...` / `go test ./...` / `golangci-lint run` 全绿；有代码改动时 `go test -race ./...` 通过（命令清单见 [.agents/go.md](../.agents/go.md) 第 3 节）。
2. spec 驱动一致性：行为/约束类改动已同步 [.spec/requirements.md](requirements.md)；实现与 spec 冲突处已先修订 spec 并获用户确认；纯 spec 变更同样按本清单收尾。
3. EARS/ADR 边界：新增/修订 Requirement 已过删除测试（无内部实现名词）；实现选择、备选与后果已落 [.spec/decisions.md](decisions.md)；被取代口径已以新 ADR 记录并标注取代关系。
4. 跨引用同步：`AGENTS.md`、`.agents/*`、`stream.go`、`.github/workflows/*`、`docs/*` 中的路径与标题引用仍有效；点目录检索使用 `--hidden`。
5. 任务勾选：[.spec/tasks.md](tasks.md) 对应任务**先勾选、后提交**（纳入同一提交）。
6. 文档同步：公开 API 变更已同步中文 godoc、`README.md`/`README_CN.md`、`docs/api.md`、`docs/design.md`、`skills/go-stream/SKILL.md`。
7. 精简与目录纪律：`.spec/` 每个规格文件 ≤300 行；未创建无实际内容的域目录；无 `legacy.md`；`待实证` 标注逐条核对（已实证销账，未实证有明确触发条件）。
8. 报错文案国际化：本次新增/变更的 panic 信息与库自建 error 字符串均为英文（测试代码例外；规范见 [.agents/general.md](../.agents/general.md) 第 4 节）。
9. 提交卫生：`git status --short` 干净；提交内容无本地绝对路径、密钥、临时文件；提交身份为本人；未经要求不 push/reset/force。

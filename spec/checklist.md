# Checklist

> 每次任务收尾时**固定执行**的验收动作，按序核对后方可提交；本清单随项目实际动态调整——过时项删除、新有用项加入，不为历史保留条目（不勾选）。

1. 质量门槛：`go fix` / `gofmt -l .` / `go vet ./...` / `go test ./...` / `golangci-lint run` 全绿（命令清单见 `agents/go.md` 第 3 节）
2. spec 一致性：行为/约束类改动已同步 `spec/spec.md`；实现与 spec 冲突处已先修订 spec 并获用户确认
3. 文档同步：公开 API 变更已同步中文 godoc、`README.md`/`README_CN.md`、`docs/api.md`、`docs/design.md`、`skills/go-stream/SKILL.md`
4. 任务勾选：`spec/tasks.md` 对应任务**先勾选、后提交**（纳入同一提交）
5. 精简核对：`spec/spec.md` ≤ 400 行；`spec/tasks.md` 无已完成且无参考价值的批次滞留；本清单仍与当前实际需要一致
6. 报错文案国际化：本次新增/变更的 panic 信息与库自建 error 字符串均为英文（测试代码例外；规范见 `agents/common.md` 第 4 节）

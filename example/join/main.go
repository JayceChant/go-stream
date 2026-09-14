// Package main 演示双流条件连接 Join/LeftJoin（Task 24，逻辑类似 SQL Join）：
// Join（方法，InnerJoin）只产出命中 on 条件的元素对；LeftJoin（方法，左外
// 连接）保证未命中的左元素以 U 零值恰好产出一次；右外连接以右流作接收者
// 调 LeftJoin 即得。
//
// 运行：go -C example run ./join（example 为独立模块，不影响库的测试与覆盖率）
package main

import (
	"fmt"

	"github.com/JayceChant/go-stream"
)

// user 与 dept 是贯穿示例的业务模型：按部门编号连接两侧数据。
type user struct {
	name string
	dept int // 部门编号（0 表示未分配）
}

type dept struct {
	id   int
	name string
}

// newUsers/newDepts 每次返回全新流：流是一次性的（连接即消费），
// 各演示段需各自构造，复用已消费的流会 panic。
func newUsers() *stream.Stream[user] {
	return stream.Of(
		user{"Alice", 1}, user{"Bob", 2}, user{"Carol", 3}, user{"Dave", 0},
	)
}

func newDepts() *stream.Stream[dept] {
	return stream.Of(
		dept{1, "研发"}, dept{2, "市场"}, dept{2, "销售"}, // 部门 2 有两个（多命中）
	)
}

func main() {
	// ---------- 1. Join：内连接（仅命中对） ----------
	// on(t, u) 返回 true 表明元素对可组合，命中对交 combine 产出；
	// 无命中的左元素（Carol/Dave）不产出。
	// 产出序左主右从：外层按左流遇序，内层按右流遇序（Bob 命中部门 2 两次）。
	fmt.Println("== Join 内连接 ==")
	rows := newUsers().
		Join(newDepts(), func(t user, u dept) bool { return t.dept == u.id },
			func(t user, u dept) string {
				return fmt.Sprintf("%s@%s", t.name, u.name)
			}).
		ToSlice()
	fmt.Println(rows) // [Alice@研发 Bob@市场 Bob@销售]

	// ---------- 2. LeftJoin：左外连接（未命中零值保底） ----------
	// 未命中的左元素以 U 零值（dept{}）恰好产出一次——SQL NULL 的 Go
	// 惯用等价物；组合函数内按零值区分未命中路径。
	fmt.Println("\n== LeftJoin 左外连接 ==")
	leftRows := stream.Of(user{"Alice", 1}, user{"Carol", 3}).
		LeftJoin(newDepts(), func(t user, u dept) bool { return t.dept == u.id },
			func(t user, u dept) string {
				if u == (dept{}) {
					return fmt.Sprintf("%s@未分配", t.name)
				}
				return fmt.Sprintf("%s@%s", t.name, u.name)
			}).
		ToSlice()
	fmt.Println(leftRows) // [Alice@研发 Carol@未分配]

	// ---------- 3. RightJoin：以右流作接收者调 LeftJoin ----------
	// 不设独立 API：交换两侧即可表达（无命中的右元素以左侧零值保底）。
	fmt.Println("\n== RightJoin（以右流调 LeftJoin）==")
	rightRows := newDepts().
		LeftJoin(stream.Of(user{"Alice", 1}), func(u dept, t user) bool { return t.dept == u.id },
			func(u dept, t user) string {
				if t == (user{}) {
					return fmt.Sprintf("空缺@%s", u.name)
				}
				return fmt.Sprintf("%s@%s", t.name, u.name)
			}).
		ToSlice()
	fmt.Println(rightRows) // [Alice@研发 空缺@市场 空缺@销售]

	// ---------- 4. 语义要点：右流物化、左流流式 ----------
	// 求值开始时右流被完整物化（右流必须有限）；左流单遍流式驱动，
	// 可为无限源——配合 Limit 等短路终止按需取前 N 条连接结果。
	fmt.Println("\n== 无限左流 + 短路 ==")
	heads := stream.Generate(func() int { return 1 }). // 恒为 1 的无限流
								Join(stream.Of(1, 2, 3), func(t, u int) bool { return u%t == 0 }, // 1 整除全部：每元素命中 3 对
				func(t, u int) int { return t*10 + u }).
		Limit(2). // 短路：首个左元素产出 2 对即停（左流只被拉动一次）
		ToSlice()
	fmt.Println(heads) // [11 12]

	// ---------- 5. 综合小案例：连接后聚合 ----------
	// 连接结果仍是流：无命中左元素映射为"未分配"后计数——LeftJoin 后 Filter+Count。
	fmt.Println("\n== 连接后聚合 ==")
	unassigned := stream.Of(user{"Alice", 1}, user{"Bob", 2}, user{"Carol", 3}).
		LeftJoin(newDepts(), func(t user, u dept) bool { return t.dept == u.id },
			func(t user, u dept) string {
				if u == (dept{}) {
					return "未分配"
				}
				return u.name
			}).
		Filter(func(deptName string) bool { return deptName == "未分配" }).
		Count()
	fmt.Println("未分配人数:", unassigned) // 1
}

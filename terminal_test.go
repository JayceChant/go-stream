package stream

import (
	"errors"
	"testing"

	"github.com/JayceChant/go-stream/collector"
)

func TestTerminalBasics(t *testing.T) {
	// ForEach / Count / ToSlice
	var sum int
	Of(1, 2, 3).ForEach(func(v int) { sum += v })
	if sum != 6 {
		t.Errorf("ForEach 累计 = %d, 期望 6", sum)
	}
	if n := Of(1, 2, 3).Count(); n != 3 {
		t.Errorf("Count = %d, 期望 3", n)
	}
	if got := Of(1, 2).ToSlice(); len(got) != 2 || got[0] != 1 {
		t.Errorf("ToSlice = %v", got)
	}

	// ForEachUntil 短路
	seen := 0
	Of(1, 2, 3).ForEachUntil(func(v int) bool {
		seen++
		return v < 2
	})
	if seen != 2 {
		t.Errorf("ForEachUntil 消费 %d 个, 期望 2", seen)
	}
}

func TestReduceFamily(t *testing.T) {
	if got := Of(1, 2, 3, 4).Reduce(0, func(a, b int) int { return a + b }); got != 10 {
		t.Errorf("Reduce = %d, 期望 10", got)
	}
	if got, ok := Of(1, 2, 3).ReduceOpt(func(a, b int) int { return a * b }); !ok || got != 6 {
		t.Errorf("ReduceOpt = %d,%v, 期望 6,true", got, ok)
	}
	if _, ok := Empty[int]().ReduceOpt(func(a, b int) int { return a }); ok {
		t.Error("空流 ReduceOpt 应返回 false")
	}
}

func TestFirstFindAny(t *testing.T) {
	if v, ok := Of(5, 6).First(); !ok || v != 5 {
		t.Errorf("First = %d,%v", v, ok)
	}
	if _, ok := Empty[int]().First(); ok {
		t.Error("空流 First 应 false")
	}
	// First 短路：无限源
	if v, ok := Iterate(10, func(v int) int { return v + 1 }).First(); !ok || v != 10 {
		t.Errorf("无限源 First = %d,%v", v, ok)
	}
	if v, ok := Of(1, 3, 6, 9).FindAny(func(v int) bool { return v%3 == 0 }); !ok || v != 3 {
		t.Errorf("FindAny = %d,%v", v, ok)
	}
}

func TestMatchFamily(t *testing.T) {
	even := func(v int) bool { return v%2 == 0 }
	// AnyMatch 短路计数
	calls := 0
	if !Of(1, 3, 4, 8).Filter(func(int) bool { calls++; return true }).AnyMatch(even) {
		t.Error("AnyMatch 应为 true")
	}
	if calls > 3 { // 1,3 不足；4 命中即停
		t.Errorf("AnyMatch 拉取 %d 次, 期望 3 次内", calls)
	}
	if !Of(2, 4).AllMatch(even) {
		t.Error("AllMatch 全偶应 true")
	}
	// AllMatch 短路：[2,4,1,8] 遇 1 即 false
	if Of(2, 4, 1, 8).AllMatch(even) {
		t.Error("AllMatch 应 false")
	}
	if !Empty[int]().AllMatch(even) {
		t.Error("空流 AllMatch 应 true")
	}
	if !Of(1, 3).NoneMatch(even) {
		t.Error("NoneMatch 应 true")
	}
	if Of(1, 2).NoneMatch(even) {
		t.Error("NoneMatch 应 false")
	}
}

func TestMinMax(t *testing.T) {
	cmpInt := func(a, b int) int { return a - b }
	if v, ok := Of(3, 1, 2).Min(cmpInt); !ok || v != 1 {
		t.Errorf("Min = %d,%v", v, ok)
	}
	if v, ok := Of(3, 1, 2).Max(cmpInt); !ok || v != 3 {
		t.Errorf("Max = %d,%v", v, ok)
	}
	if _, ok := Empty[int]().Min(cmpInt); ok {
		t.Error("空流 Min 应 false")
	}
}

func TestErrQuery(t *testing.T) {
	boom := errors.New("boom")
	s := Of("1", "x").MapErr(func(s string) (int, error) {
		if s == "x" {
			return 0, boom
		}
		return 1, nil
	})
	s.ToSlice() // 部分结果
	if !errors.Is(s.Err(), boom) {
		t.Errorf("Err() = %v, 期望 boom", s.Err())
	}
	if e := Of(1, 2).Err(); e != nil {
		t.Errorf("无错流 Err() = %v, 期望 nil", e)
	}
}

func TestCollectAndCollectors(t *testing.T) {
	// Collect + ToSlice
	if got := Of(1, 2, 3).Collect(collector.ToSlice[int]()); len(got) != 3 {
		t.Errorf("Collect(ToSlice) = %v", got)
	}
	// ToSet
	set := Of(1, 2, 2, 3).Collect(collector.ToSet[int]())
	if len(set) != 3 {
		t.Errorf("ToSet = %v, 期望 3 键", set)
	}
	// ToMap last-wins
	m := Of("a", "b", "a2").Collect(collector.ToMap(
		func(s string) rune { return []rune(s)[0] },
		func(s string) int { return len(s) },
	))
	if m['a'] != 2 { // a2 覆盖 a
		t.Errorf("ToMap last-wins: m['a'] = %d, 期望 2", m['a'])
	}
	// ToMapMerge
	m2 := Of("a", "b", "a2").Collect(collector.ToMapMerge(
		func(s string) rune { return []rune(s)[0] },
		func(s string) int { return len(s) },
		func(oldV, newV int) int { return oldV + newV },
	))
	if m2['a'] != 3 { // 1+2
		t.Errorf("ToMapMerge: m2['a'] = %d, 期望 3", m2['a'])
	}
}

type emp struct {
	dept string
	name string
	sal  int
}

func TestGroupingByOrdering(t *testing.T) {
	emps := []emp{
		{"dev", "张三", 100},
		{"ops", "李四", 80},
		{"dev", "王五", 120},
		{"ops", "赵六", 90},
	}
	g := FromSlice(emps).Collect(collector.GroupingBy(
		func(e emp) string { return e.dept },
		func(e emp) string { return e.name },
	))
	if len(g["dev"]) != 2 || g["dev"][0] != "张三" || g["dev"][1] != "王五" {
		t.Errorf("分组保序失败: %v", g["dev"])
	}
	if g["ops"][1] != "赵六" {
		t.Errorf("ops 组 = %v", g["ops"])
	}
}

func TestJoiningCountingReducing(t *testing.T) {
	if got := Of(1, 2, 3).Collect(collector.Joining(func(v int) string {
		digits := []string{"0", "1", "2", "3"}
		return digits[v]
	}, "-")); got != "1-2-3" {
		t.Errorf("Joining = %q", got)
	}
	if n := Of(1, 2, 3).Collect(collector.Counting[int]()); n != 3 {
		t.Errorf("Counting = %d", n)
	}
	if got := Of(1, 2, 3, 4).Collect(collector.Reducing(0, func(a, b int) int { return a + b })); got != 10 {
		t.Errorf("Reducing = %d", got)
	}
}

func TestMappingCombinator(t *testing.T) {
	// Mapping + GroupingBy：按部门分组工资
	emps := []emp{{"dev", "a", 1}, {"ops", "b", 2}, {"dev", "c", 3}}
	g := FromSlice(emps).Collect(collector.Mapping(
		func(e emp) KV[string, int] { return KV[string, int]{e.dept, e.sal} },
		collector.GroupingBy(
			func(kv KV[string, int]) string { return kv.Key },
			func(kv KV[string, int]) int { return kv.Value },
		),
	))
	if len(g["dev"]) != 2 || g["dev"][0] != 1 {
		t.Errorf("Mapping+GroupingBy = %v", g)
	}
}

func TestSummingCollector(t *testing.T) {
	if got := Of(1, 2, 3).Collect(Summing[int]()); got != 6 {
		t.Errorf("Summing = %d", got)
	}
}

func TestPackageHelpers(t *testing.T) {
	// Sum / Avg
	if got := Sum(Range(0, 100)); got != 4950 {
		t.Errorf("Sum = %d, 期望 4950", got)
	}
	if got := Avg(Range(0, 4)); got != 1 { // (0+1+2+3)/4
		t.Errorf("Avg = %d, 期望 1", got)
	}
	// Contains 短路
	calls := 0
	if !Contains(Of(1, 2, 3).Peek(func(int) { calls++ }), 2) {
		t.Error("Contains 应 true")
	}
	if calls != 2 {
		t.Errorf("Contains 拉取 %d 次, 期望 2（短路）", calls)
	}
	// 包级 Sorted/Min/Max（cmp.Ordered）
	if got := Sorted(Of(3, 1, 2)).ToSlice(); len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("Sorted = %v", got)
	}
	if v, ok := Min(Of(3, 1, 2)); !ok || v != 1 {
		t.Errorf("Min = %d,%v", v, ok)
	}
	if v, ok := Max(Of(3, 1, 2)); !ok || v != 3 {
		t.Errorf("Max = %d,%v", v, ok)
	}
	// 字符串自然序
	if got := Sorted(Of("c", "a", "b")).ToSlice(); got[0] != "a" {
		t.Errorf("字符串 Sorted = %v", got)
	}
}

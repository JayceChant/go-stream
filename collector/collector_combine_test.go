package collector

import (
	"slices"
	"testing"
)

// collector_combine_test.go：Task 20——Collector 组合生态单测。

func acc[T any, A any, R any](c Collector[T, A, R], in []T) R {
	a := c.Supplier()
	for _, v := range in {
		c.Accumulator(a, v)
	}
	return c.Finisher(a)
}

func combine[T any, A any, R any](c Collector[T, A, R], a, b []T) R {
	acc1, acc2 := c.Supplier(), c.Supplier()
	for _, v := range a {
		c.Accumulator(acc1, v)
	}
	for _, v := range b {
		c.Accumulator(acc2, v)
	}
	com := c.Combiner()
	if com == nil {
		panic("combiner 为 nil")
	}
	return c.Finisher(com(acc1, acc2))
}

func TestGroupingByDownstream(t *testing.T) {
	// 分组计数：偶数组计数 2、奇数组计数 3
	got := acc(GroupingByDownstream(func(v int) string {
		if v%2 == 0 {
			return "even"
		}
		return "odd"
	}, Counting[int]()), []int{1, 2, 3, 4, 5})
	if got["even"] != 2 || got["odd"] != 3 {
		t.Errorf("分组计数 = %v", got)
	}

	// 分组求和 + 组内遇序（Joining）
	gotS := acc(GroupingByDownstream(func(v int) string {
		if v%2 == 0 {
			return "even"
		}
		return "odd"
	}, Mapping(func(v int) string { return string(rune('0' + v) /*n 先转字符*/) }, Joining(func(s string) string { return s }, ","))),
		[]int{1, 2, 3, 4})
	if gotS["even"] != "2,4" || gotS["odd"] != "1,3" {
		t.Errorf("分组拼接 = %v", gotS)
	}

	// Combiner 并行合并：分片各自累积后合并与整段一致
	gotC := combine(GroupingByDownstream(func(v int) int { return v % 2 }, Counting[int]()),
		[]int{1, 2, 3}, []int{4, 5, 6})
	if gotC[0] != 3 || gotC[1] != 3 {
		t.Errorf("分片合并 = %v", gotC)
	}

	// 下游 Combiner 为 nil 时整体降级串行
	c := GroupingByDownstream(func(v int) int { return v }, nilCombinerCollector[int]{})
	if c.Combiner() != nil {
		t.Error("下游无 Combiner 时整体应返回 nil")
	}
}

// nilCombinerCollector 是 Combiner 返回 nil 的测试用下游。
type nilCombinerCollector[T any] struct{}

func (nilCombinerCollector[T]) Supplier() *[]T                  { return new([]T) }
func (nilCombinerCollector[T]) Accumulator(a *[]T, v T)         { *a = append(*a, v) }
func (nilCombinerCollector[T]) Combiner() func(*[]T, *[]T) *[]T { return nil }
func (nilCombinerCollector[T]) Finisher(a *[]T) []T             { return *a }

func TestPartitioningBy(t *testing.T) {
	got := acc(PartitioningBySlice(func(v int) bool { return v%2 == 0 }), []int{1, 2, 3, 4, 6})
	if !slices.Equal(got.True, []int{2, 4, 6}) || !slices.Equal(got.False, []int{1, 3}) {
		t.Errorf("PartitioningBySlice = %+v", got)
	}

	// 空侧恒非 nil（零值结果）
	gotE := acc(PartitioningBySlice(func(v int) bool { return true }), []int{1, 2})
	if len(gotE.False) != 0 {
		t.Errorf("空 False 侧应为 nil 切片, got %v", gotE.False)
	}

	// 下游 Counting：两侧各自计数
	gotC := acc(PartitioningBy(func(v int) bool { return v > 2 }, Counting[int]()), []int{1, 2, 3, 4})
	if gotC.True != 2 || gotC.False != 2 {
		t.Errorf("PartitioningBy+Counting = %+v", gotC)
	}

	// Combiner 合并
	gotM := combine(PartitioningBySlice(func(v int) bool { return v%2 == 0 }),
		[]int{1, 2}, []int{3, 4})
	if !slices.Equal(gotM.True, []int{2, 4}) || !slices.Equal(gotM.False, []int{1, 3}) {
		t.Errorf("分片合并 = %+v", gotM)
	}
}

func TestTeeing(t *testing.T) {
	// 一次遍历：计数 + 拼接 → 合并
	c := Teeing(Counting[int](), Joining(func(v int) string {
		return string(rune('0' + v))
	}, ","), func(n int64, s string) string {
		return s
	})
	got := acc(c, []int{1, 2, 3})
	if got != "1,2,3" {
		t.Errorf("Teeing = %q", got)
	}

	// Combiner 合并（双侧可并行）
	gotC := combine(Teeing(Summing[int](), Counting[int](), func(s int, n int64) int64 {
		return int64(s) * n
	}), []int{1, 2}, []int{3, 4})
	if gotC != 40 { // (1+2+3+4)*(2+2)
		t.Errorf("Teeing 合并 = %d", gotC)
	}

	// 一侧 Combiner nil → 整体 nil（降级串行）
	cn := Teeing(Counting[int](), nilCombinerCollector[int]{}, func(n int64, s []int) int64 { return n })
	if cn.Combiner() != nil {
		t.Error("任一下游无 Combiner 时整体应返回 nil")
	}
}

func TestFilteringFlatMapping(t *testing.T) {
	got := acc(Filtering(func(v int) bool { return v%2 == 0 }, ToSlice[int]()), []int{1, 2, 3, 4})
	if !slices.Equal(got, []int{2, 4}) {
		t.Errorf("Filtering = %v", got)
	}

	gotF := acc(FlatMapping(func(v int) []string {
		if v == 1 {
			return []string{"a", "b"}
		}
		return nil
	}, ToSlice[string]()), []int{1, 2, 1})
	if !slices.Equal(gotF, []string{"a", "b", "a", "b"}) {
		t.Errorf("FlatMapping = %v", gotF)
	}

	// 嵌套：GroupingByDownstream(FlatMapping(ToSlice))
	gotG := acc(GroupingByDownstream(func(v int) string { return "g" },
		FlatMapping(func(v int) []int { return []int{v, v * 10} }, ToSlice[int]())),
		[]int{1, 2})
	if !slices.Equal(gotG["g"], []int{1, 10, 2, 20}) {
		t.Errorf("嵌套组合 = %v", gotG)
	}
}

func TestCollectingAndThen(t *testing.T) {
	got := acc(CollectingAndThen(ToSlice[int](), func(s []int) int { return len(s) }), []int{1, 2, 3})
	if got != 3 {
		t.Errorf("CollectingAndThen = %d", got)
	}
}

func TestMinMaxBy(t *testing.T) {
	if got := acc(MinBy(func(a, b int) int { return a - b }), []int{3, 1, 2}); got != 1 {
		t.Errorf("MinBy = %d", got)
	}
	if got := acc(MaxBy(func(a, b int) int { return a - b }), []int{3, 1, 2}); got != 3 {
		t.Errorf("MaxBy = %d", got)
	}
	// 空流返回零值
	if got := acc(MinBy(func(a, b int) int { return a - b }), nil); got != 0 {
		t.Errorf("空流 MinBy = %d", got)
	}
	// Combiner 分片合并
	gotC := combine(MaxBy(func(a, b int) int { return a - b }), []int{3, 1}, []int{5, 2})
	if gotC != 5 {
		t.Errorf("MaxBy 合并 = %d", gotC)
	}
}

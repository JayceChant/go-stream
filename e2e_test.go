package stream

// e2e_test.go：端到端组合场景测试（模拟 Java Stream 典型用法）。

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
)

// 场景 1：filter → map → collect（Java 最典型三段式）。
func TestE2E_FilterMapCollect(t *testing.T) {
	got := Of(1, 2, 3, 4, 5, 6).
		Filter(func(n int) bool { return n%2 == 0 }).
		Map(strconv.Itoa).
		Collect(ToSlice[string]())
	want := []string{"2", "4", "6"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// 场景 2：分组聚合 GroupingBy + Mapping 组合子。
func TestE2E_GroupingBy(t *testing.T) {
	type person struct {
		name string
		age  int
	}
	ps := []person{
		{"Alice", 30}, {"Bob", 25}, {"Carol", 30}, {"Dave", 25},
	}
	byAge := FromSlice(ps).Collect(GroupingBy(
		func(p person) int { return p.age },
		func(p person) string { return p.name },
	))
	want := map[int][]string{
		25: {"Bob", "Dave"},
		30: {"Alice", "Carol"},
	}
	for age, names := range want {
		if !slices.Equal(byAge[age], names) {
			t.Fatalf("age %d: got %v, want %v", age, byAge[age], names)
		}
	}
}

// 场景 3：无限流 + Limit 短路。
func TestE2E_InfiniteTake(t *testing.T) {
	got := Generate(func() int { return 42 }).
		Limit(3).
		ToSlice()
	if len(got) != 3 || got[2] != 42 {
		t.Fatalf("got %v", got)
	}
}

// 场景 4：错误管道——部分结果 + Err() 查询。
func TestE2E_ErrorPipeline(t *testing.T) {
	s := FromFunc(func() (int, bool, error) {
		return 0, false, errors.New("boom")
	})
	got := s.ToSlice()
	if len(got) != 0 {
		t.Fatalf("expect empty partial result, got %v", got)
	}
	if s.Err() == nil || s.Err().Error() != "boom" {
		t.Fatalf("Err() = %v", s.Err())
	}
}

// 场景 5：排序 + 去重 + 分页（Skip/Limit）组合。
func TestE2E_SortDistinctPage(t *testing.T) {
	got := Of(5, 3, 1, 3, 5, 2, 1).
		Sorted(func(a, b int) int { return a - b }).
		DistinctBy(func(n int) any { return n }).
		Skip(1).
		Limit(3).
		ToSlice()
	if !slices.Equal(got, []int{2, 3, 5}) {
		t.Fatalf("got %v", got)
	}
}

// 场景 6：Zip 双流 + 数值聚合。
func TestE2E_ZipAggregate(t *testing.T) {
	names := Of("a", "b", "c")
	// Range(1,100) 长于 names，Zip 取短
	pairs := names.Zip(Range(1, 100), func(s string, i int) string {
		return fmt.Sprintf("%s%d", s, i)
	}).ToSlice()
	if !slices.Equal(pairs, []string{"a1", "b2", "c3"}) {
		t.Fatalf("got %v", pairs)
	}
}

// 场景 7：文字统计——FromMap + FlatMap + Joining。
func TestE2E_Joining(t *testing.T) {
	got := Of(1, 2, 3).
		Collect(Joining(strconv.Itoa, "-"))
	if got != "1-2-3" {
		t.Fatalf("got %q", got)
	}
}

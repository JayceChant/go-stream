package stream_test

// example_test.go：可运行示例（与 README.md / docs/api.md 示例保持一致）。

import (
	"fmt"
	"strconv"

	"github.com/JayceChant/go-stream"
)

// 基础链式：构造 → 中间操作 → 终止操作。
func ExampleOf() {
	got := stream.Of(1, 2, 3, 4, 5).
		Filter(func(n int) bool { return n%2 == 1 }).
		Map(func(n int) int { return n * n }).
		ToSlice()
	fmt.Println(got)
	// Output: [1 9 25]
}

// 类型迁移：Map 把 int 流变为 string 流（编译期静态类型检查）。
func Example_mapTypeChange() {
	got := stream.Of(1, 2, 3).
		Map(strconv.Itoa).
		Collect(stream.Joining(func(s string) string { return s }, "-"))
	fmt.Println(got)
	// Output: 1-2-3
}

// Collect 与预置收集器：分组（组内保序）。
func ExampleGroupingBy() {
	type person struct {
		name string
		age  int
	}
	byAge := stream.Of(
		person{"Alice", 30}, person{"Bob", 25},
		person{"Carol", 30}, person{"Dave", 25},
	).Collect(stream.GroupingBy(
		func(p person) int { return p.age },
		func(p person) string { return p.name },
	))
	for _, age := range []int{25, 30} {
		fmt.Println(age, byAge[age])
	}
	// Output:
	// 25 [Bob Dave]
	// 30 [Alice Carol]
}

// 无限流 + 短路：Generate 搭配 Limit。
func Example_infiniteWithLimit() {
	i := 0
	got := stream.Generate(func() int { i++; return i * i }).
		Limit(5).
		ToSlice()
	fmt.Println(got)
	// Output: [1 4 9 16 25]
}

// 数值聚合：包级 Sum/Avg（方法无法追加 Number 约束）。
func Example_numeric() {
	fmt.Println(stream.Sum(stream.Range(1, 101)))
	fmt.Println(stream.Avg(stream.Range(1, 4)))
	// Output:
	// 5050
	// 2
}

// 错误即值：FromFunc 源失败时部分结果保留，Err() 查询首错。
func Example_errorAsValue() {
	n := 0
	s := stream.FromFunc(func() (int, bool, error) {
		n++
		if n >= 4 {
			return 0, false, fmt.Errorf("第 %d 次读取失败", n)
		}
		return n, true, nil
	})
	got := s.ToSlice()
	fmt.Println(got)
	fmt.Println(s.Err())
	// Output:
	// [1 2 3]
	// 第 4 次读取失败
}

// 排序去重分页组合。
func Example_sortedDistinctPage() {
	got := stream.Of(5, 3, 1, 3, 5, 2, 1).
		Sorted(func(a, b int) int { return a - b }).
		DistinctBy(func(n int) any { return n }).
		Skip(1).
		Limit(3).
		ToSlice()
	fmt.Println(got)
	// Output: [2 3 5]
}

// 双流拉链：Zip 取短。
func Example_zip() {
	got := stream.Of("a", "b", "c").
		Zip(stream.Range(1, 100), func(s string, i int) string {
			return fmt.Sprintf("%s%d", s, i)
		}).
		ToSlice()
	fmt.Println(got)
	// Output: [a1 b2 c3]
}

// map 源：FromMap 产出 KV 键值对。
func Example_fromMap() {
	m := map[string]int{"one": 1, "two": 2}
	total := 0
	stream.FromMap(m).ForEach(func(kv stream.KV[string, int]) {
		total += kv.Value
	})
	fmt.Println(total)
	// Output: 3
}

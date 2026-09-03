// Package main 演示 go-stream 的基础用法：构造 → 中间操作 → 终止操作。
//
// 运行：go -C example run ./basics（example 为独立模块，不影响库的测试与覆盖率）
//
// 流（Stream）是一条惰性管道：中间操作只追加阶段不触发遍历，
// 终止操作触发一次单遍求值。同一流只能消费一次，重复消费会 panic。
package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/JayceChant/go-stream"
)

// base 是本示例反复用到的基准数据。流是一次性的，
// 每次终止求值前都从 base 构造一条新流。
var base = []int{5, 3, 9, 1, 7}

func main() {
	// ---------- 1. 最典型的链式调用 ----------
	oddSquares := stream.Of(1, 2, 3, 4, 5).
		Filter(func(n int) bool { return n%2 == 1 }). // 1 3 5
		Map(func(n int) int { return n * n }).        // 1 9 25
		ToSlice()
	fmt.Println("Filter+Map+ToSlice:", oddSquares)

	// ---------- 2. 多种数据源 ----------
	// FromSlice：零拷贝引用现有切片
	fmt.Println("FromSlice:", stream.FromSlice([]string{"a", "b", "c"}).Count())

	// FromSeq：适配 Go 1.23 push 迭代器（如 slices.Values）
	fmt.Println("FromSeq:", stream.FromSeq(slices.Values([]int{10, 20, 30})).ToSlice())

	// FromChannel：阻塞拉取直到通道关闭
	ch := make(chan int, 3)
	for _, v := range []int{7, 8, 9} {
		ch <- v
	}
	close(ch)
	fmt.Println("FromChannel:", stream.FromChannel(ch).ToSlice())

	// FromMap：产出 KV 键值对（map 遍历序不确定，为无序流）
	ages := map[string]int{"Alice": 30, "Bob": 25}
	stream.FromMap(ages).ForEach(func(kv stream.KV[string, int]) {
		fmt.Printf("FromMap: %s=%d\n", kv.Key, kv.Value)
	})

	// Range：整数区间 [start, stop)
	fmt.Println("Range(1,6):", stream.Range(1, 6).ToSlice())

	// Concat：串联两条流
	fmt.Println("Concat:", stream.Concat(stream.Of(1, 2), stream.Of(3, 4)).ToSlice())

	// ---------- 3. 无状态中间操作 ----------
	// FlatMap：一对多展开（如把句子拆成单词）
	words := stream.Of("go stream rocks", "streams are lazy").
		FlatMap(strings.Fields).
		ToSlice()
	fmt.Println("FlatMap 拆词:", words)

	// Peek：副作用观察（常用于调试），不改变元素
	traced := stream.Of(1, 2, 3).
		Peek(func(n int) { fmt.Println("  Peek 看到:", n) }).
		Map(func(n int) int { return n * 10 }).
		ToSlice()
	fmt.Println("Peek+Map:", traced)

	// TakeWhile / DropWhile：按前缀条件截取/丢弃（TakeWhile 短路）
	src := []int{1, 3, 5, 2, 4}
	fmt.Println("TakeWhile<4:", stream.FromSlice(src).
		TakeWhile(func(n int) bool { return n < 4 }).
		ToSlice()) // [1 3]
	fmt.Println("DropWhile<4:", stream.FromSlice(src).
		DropWhile(func(n int) bool { return n < 4 }).
		ToSlice()) // [5 2 4]

	// ---------- 4. 有状态中间操作 ----------
	scores := []int{88, 72, 95, 60, 88, 79}

	// Sorted：稳定排序（比较器对齐 slices.SortFunc 惯例：负/零/正）
	fmt.Println("Sorted 升序:", stream.FromSlice(scores).
		Sorted(cmp.Compare[int]).ToSlice())
	fmt.Println("Sorted 降序:", stream.FromSlice(scores).
		Sorted(func(a, b int) int { return cmp.Compare(b, a) }).ToSlice())

	// DistinctBy：按 key 去重，保留首见（元素本身无需可比较）
	type student struct {
		id   int
		name string
	}
	students := stream.Of(
		student{1, "Alice"}, student{2, "Bob"}, student{3, "Alice"},
	)
	fmt.Println("DistinctBy name:", students.
		DistinctBy(func(s student) string { return s.name }).
		ToSlice())

	// Limit / Skip：截取与跳过（Limit 对无限源可短路终止）
	fmt.Println("Skip(1)+Limit(3):", stream.Range(0, 10).
		Skip(1).
		Limit(3).
		ToSlice())

	// Reverse：反转
	fmt.Println("Reverse:", stream.Of(1, 2, 3).
		Reverse().
		ToSlice())

	// ---------- 5. 终止操作 ----------
	fmt.Println("Count:", stream.FromSlice(base).Count())

	fmt.Println("Reduce 求和:", stream.FromSlice(base).
		Reduce(0, func(acc, n int) int { return acc + n }))

	product, ok := stream.FromSlice(base).ReduceOpt(func(a, b int) int { return a * b })
	fmt.Println("ReduceOpt 求积:", product, ok)

	first, _ := stream.FromSlice(base).First()
	fmt.Println("First:", first)

	fmt.Println("AnyMatch>8:", stream.FromSlice(base).AnyMatch(func(n int) bool { return n > 8 }))
	fmt.Println("AllMatch<10:", stream.FromSlice(base).AllMatch(func(n int) bool { return n < 10 }))
	fmt.Println("NoneMatch>9:", stream.FromSlice(base).NoneMatch(func(n int) bool { return n > 9 }))

	lowest, _ := stream.FromSlice(base).Min(cmp.Compare[int])
	greatest, _ := stream.FromSlice(base).Max(cmp.Compare[int])
	fmt.Println("Min/Max:", lowest, greatest)

	// ForEachUntil：回调返回 false 提前终止（消费前 3 个即停）
	var seen []int
	stream.FromSlice(base).ForEachUntil(func(v int) bool {
		seen = append(seen, v)
		return len(seen) < 3
	})
	fmt.Println("ForEachUntil 前 3 个:", seen)
}

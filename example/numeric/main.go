// Package main 演示数值场景：包级聚合函数、Scan 前缀和、无限源、Zip、Chunk/Enumerate。
//
// 运行：go -C example run ./numeric（example 为独立模块，不影响库的测试与覆盖率）
//
// Go 泛型方法无法对接收者的 T 追加 Number/cmp.Ordered 约束，
// 因此数值聚合与"免写比较器"形态以包级函数提供（stream.Sum 等）。
package main

import (
	"fmt"

	"github.com/JayceChant/go-stream"
)

func main() {
	// ---------- 1. 包级数值聚合 ----------
	fmt.Println("Sum(1..100):", stream.Sum(stream.Range(1, 101)))
	fmt.Println("Avg(1..3):", stream.Avg(stream.Range(1, 4)))

	// 免写比较器的 Sorted/Min/Max（cmp.Ordered 约束）
	fmt.Println("Sorted:", stream.Sorted(stream.Of(3, 1, 2)).ToSlice())
	lo, _ := stream.Min(stream.Of(3, 1, 2))
	hi, _ := stream.Max(stream.Of(3, 1, 2))
	fmt.Println("Min/Max:", lo, hi)

	// Contains：短路查找（comparable 约束）
	fmt.Println("Contains(2):", stream.Contains(stream.Range(0, 10), 2))

	// ---------- 2. Scan：滚动累积（前缀和） ----------
	// 输出含初值：0, 0+1, 0+1+2, ...
	prefix := stream.Of(1, 2, 3, 4).
		Scan(0, func(acc, n int) int { return acc + n }).
		ToSlice()
	fmt.Println("前缀和:", prefix) // [0 1 3 6 10]

	// ---------- 3. 无限源：Generate / Iterate + Limit 短路 ----------
	i := 0
	squares := stream.Generate(func() int { i++; return i * i }).
		Limit(5).
		ToSlice()
	fmt.Println("Generate 前 5 个平方数:", squares) // [1 4 9 16 25]

	fibs := stream.Iterate([]int{0, 1}, func(p []int) []int {
		return []int{p[1], p[0] + p[1]}
	})
	fibPairs := fibs.
		Limit(10).
		ToSlice()
	fibN := make([]int, len(fibPairs))
	for i, p := range fibPairs {
		fibN[i] = p[0]
	}
	fmt.Println("斐波那契前 10 项:", fibN)

	// ---------- 4. Zip：双流按位置配对（取短） ----------
	names := stream.Of("Alice", "Bob", "Carol")
	scores := stream.Of(90, 85)
	pairs := names.
		Zip(scores, func(n string, s int) string {
			return fmt.Sprintf("%s=%d", n, s)
		}).
		ToSlice()
	fmt.Println("Zip 配对（取短）:", pairs) // [Alice=90 Bob=85]

	// ---------- 5. Chunk：定长分批（批量写库/分页高频） ----------
	batches := stream.Chunk(stream.Range(1, 10), 4).ToSlice()
	fmt.Println("Chunk(4):", batches) // [1 2 3 4] [5 6 7 8] [9]

	// ---------- 6. Enumerate：附加索引（对应 for i, v := range） ----------
	stream.Enumerate(stream.Of("a", "b", "c")).
		ForEach(func(kv stream.KV[int, string]) {
			fmt.Printf("Enumerate: %d:%s\n", kv.Key, kv.Value)
		})

	// ---------- 7. Distinct：元素自身可比较的去重（包级，保遇序） ----------
	fmt.Println("Distinct:", stream.Distinct(stream.Of(3, 1, 3, 2, 1)).
		ToSlice()) // [3 1 2]

	// ---------- 8. 综合小案例：移动平均 ----------
	// 滑动窗口 3 的移动平均 = 前缀和差分；Scan 不物化、单遍完成。
	window := 3
	data := []float64{1, 2, 3, 4, 5}
	// 先算前缀和（含初值），再按窗口差分
	sums := stream.FromSlice(data).
		Scan(0.0, func(acc, v float64) float64 { return acc + v }).
		ToSlice()
	var ma []float64
	for i := window; i < len(sums); i++ {
		ma = append(ma, (sums[i]-sums[i-window])/float64(window))
	}
	fmt.Println("移动平均(w=3):", ma) // [2 3 4]
}

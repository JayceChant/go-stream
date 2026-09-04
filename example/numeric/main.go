// Package main 演示数值场景：NumberStream 数值流链式聚合、包级函数对照、
// Scan 前缀和、无限源、Zip、Chunk/Enumerate。
//
// 运行：go -C example run ./numeric（example 为独立模块，不影响库的测试与覆盖率）
//
// 依赖元素约束的 API（Sum/Avg/Min/Max/Contains/自然序 Sorted/Distinct）由
// NumberStream 数值流承载方法形态；普通 *Stream 继续用包级函数（并存≠重复）。
package main

import (
	"fmt"

	"github.com/JayceChant/go-stream"
)

func main() {
	// ---------- 1. NumberStream：链式数值聚合（Range 直接收窄） ----------
	fmt.Println("Sum(1..100):", stream.Range(1, 101).Sum())
	fmt.Println("Avg(1..3):", stream.Range(1, 4).Avg())

	// 收窄红利：自然序 Sorted/StableSorted/Distinct 免写比较器/键函数
	fmt.Println("Sorted:", stream.OfNumber(3, 1, 2).Sorted().ToSlice())
	lo, _ := stream.OfNumber(3, 1, 2).Min()
	hi, _ := stream.OfNumber(3, 1, 2).Max()
	fmt.Println("Min/Max:", lo, hi)
	fmt.Println("Contains(2):", stream.Range(0, 10).Contains(2))
	fmt.Println("Distinct:", stream.OfNumber(3, 1, 3, 2, 1).
		Distinct().Sorted().ToSlice()) // [1 2 3]

	// FromNumberSlice：slice 收窄入口；StableSorted 稳定自然序；并行 Max
	fmt.Println("FromNumberSlice+StableSorted:", stream.FromNumberSlice([]int{5, 3, 9, 1}).
		StableSorted().ToSlice()) // [1 3 5 9]
	if hi, ok := stream.Range(0, 1000).Parallel(4).Max(); ok {
		fmt.Println("Parallel(4).Max:", hi) // 999
	}

	// 类型迁移入窄流：MapToNumber（对应 Java mapToInt）
	words := []string{"go", "stream", "number"}
	fmt.Println("MapToNumber(len).Sum():",
		stream.FromSlice(words).MapToNumber(func(s string) int {
			return len(s)
		}).Sum()) // 2+6+6=14

	// 双向桥接：AsNumber 收窄 / AsStream 逃逸（Zip 另一侧等场景）
	fmt.Println("AsNumber().Filter().Sum():",
		stream.AsNumber(stream.Of(1, 2, 3, 4)).Filter(func(v int) bool {
			return v%2 == 0
		}).Sum()) // 6
	pairs := stream.Of("a", "b").
		Zip(stream.Range(1, 10).AsStream(), func(s string, i int) string {
			return fmt.Sprintf("%s%d", s, i)
		}).
		ToSlice()
	fmt.Println("Zip(AsStream):", pairs) // [a1 b2]

	// ---------- 2. 包级函数对照（普通 *Stream 的便捷形态，继续可用） ----------
	fmt.Println("包级 Sum:", stream.Sum(stream.Of(1, 2, 3)))
	fmt.Println("包级 Contains:", stream.Contains(stream.Of(1, 2, 3), 2))

	// ---------- 3. Scan：滚动累积（前缀和） ----------
	// 输出含初值：0, 0+1, 0+1+2, ...
	prefix := stream.Of(1, 2, 3, 4).
		Scan(0, func(acc, n int) int { return acc + n }).
		ToSlice()
	fmt.Println("前缀和:", prefix) // [0 1 3 6 10]

	// ---------- 4. 无限源：Generate / Iterate + Limit 短路 ----------
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

	// ---------- 5. Zip：双流按位置配对（取短） ----------
	names := stream.Of("Alice", "Bob", "Carol")
	scores := stream.Of(90, 85)
	zipPairs := names.
		Zip(scores, func(n string, s int) string {
			return fmt.Sprintf("%s=%d", n, s)
		}).
		ToSlice()
	fmt.Println("Zip 配对（取短）:", zipPairs) // [Alice=90 Bob=85]

	// ---------- 6. Chunk：定长分批（批量写库/分页高频） ----------
	batches := stream.Chunk(stream.Range(1, 10).AsStream(), 4).ToSlice()
	fmt.Println("Chunk(4):", batches) // [1 2 3 4] [5 6 7 8] [9]

	// ---------- 7. Enumerate：附加索引（对应 for i, v := range） ----------
	stream.Enumerate(stream.Of("a", "b", "c")).
		ForEach(func(kv stream.KV[int, string]) {
			fmt.Printf("Enumerate: %d:%s\n", kv.Key, kv.Value)
		})

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

package stream

import "github.com/JayceChant/go-stream/collector"

// collector.go（根包侧）：依赖根包 Number 约束的收集器。
//
// 其余收集器族（Collector 类型与 ToSlice/ToSet/ToMap/ToMapMerge/
// GroupingBy/Joining/Counting/Reducing/Mapping）已迁移至低耦合子包
// stream/collector（零依赖叶子包，见 spec「包结构」）；本文件仅保留
// 因依赖 Number 约束而无法迁出的 Summing（与 numeric.go 的 Sum/Avg
// 同属数值聚合族）。

// Summing 数值求和收集器（Number 约束的便捷形态）。
// 依赖根包 Number 约束故留根包；与子包 collector.Collector 兼容。
func Summing[N Number]() collector.Collector[N, *N, N] {
	return collector.Collector[N, *N, N]{
		Supplier:    func() *N { return new(N) },
		Accumulator: func(a *N, v N) { *a += v },
		Combiner: func(a, b *N) *N {
			*a += *b
			return a
		},
		Finisher: func(a *N) N { return *a },
	}
}

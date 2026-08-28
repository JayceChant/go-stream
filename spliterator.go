package stream

// Characteristics 是数据源结构特征的位标志。
//
// 求值器可依据特征位省略不必要的工作（如源已 Sorted 则跳过排序）；
// 后续并行实现（见 spec 后续 TODO）将依赖 Sized/SubSized 做均衡拆分。
type Characteristics uint16

const (
	// SpSized 表示源可精确报告元素数量（EstimateSize 返回精确值）。
	SpSized Characteristics = 1 << iota
	// SpOrdered 表示源具有确定的相遇顺序，遍历与拆分必须保持该顺序。
	SpOrdered
	// SpSubSized 表示 TrySplit 产生的子源同样可精确报告大小（仅在 SpSized 时有意义）。
	SpSubSized
	// SpSorted 表示源元素按某比较器有序。
	SpSorted
	// SpDistinct 表示源元素两两不重复。
	SpDistinct
)

// Splitterator 是流的数据源抽象：可逐个推进、可整体遍历、可分裂（为并行预留）。
//
// 概念对应 Java 的 Spliterator（Go 侧按拼写惯例写作 Splitterator）。
// 一个 Splitterator 只服务于一次求值：遍历与分裂都会消耗其元素。
type Splitterator[T any] interface {
	// TryAdvance 尝试推进到下一个元素：存在则以该元素调用 f 一次并返回 true；
	// f 返回 false 表示消费方请求短路（实现不得再调用 f）；元素耗尽返回 false。
	TryAdvance(f func(T) bool) bool
	// ForEachRemaining 遍历剩余全部元素并逐个调用 f；f 返回 false 时提前结束。
	// 默认可基于 TryAdvance 实现，高效源应自行提供。
	ForEachRemaining(f func(T) bool)
	// TrySplit 尝试把剩余元素分裂出一个新的 Splitterator 供并行处理；
	// 不可分裂（或分裂无益）时返回 nil。串行实现恒为 nil。
	TrySplit() Splitterator[T]
	// EstimateSize 返回剩余元素估计数；未知返回 -1。Sized 特征下为精确值。
	EstimateSize() int64
	// Characteristics 返回本源的特征位集合。
	Characteristics() Characteristics
}

// baseSplitterator 为各数据源实现提供公共字段的嵌入基座（组合，非继承）。
//
// 嵌入者获得：默认 TrySplit（不可分裂）、EstimateSize 与 Characteristics 的
// 字段直读实现；只需自行实现 TryAdvance 与 ForEachRemaining，
// 可分裂源（如 slice/range）再按需覆写 TrySplit。
type baseSplitterator[T any] struct {
	estSize int64           // 剩余元素估计数，-1 表示未知
	chars   Characteristics // 源特征位
}

// TrySplit 默认不可分裂，返回 nil。
func (b *baseSplitterator[T]) TrySplit() Splitterator[T] { return nil }

// EstimateSize 返回构造时给定的估计数。
func (b *baseSplitterator[T]) EstimateSize() int64 { return b.estSize }

// Characteristics 返回构造时给定的特征位。
func (b *baseSplitterator[T]) Characteristics() Characteristics { return b.chars }

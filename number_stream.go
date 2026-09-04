package stream

import "cmp"

// number_stream.go：NumberStream——Number 约束的数值流类型（对标 Java IntStream/LongStream）。
//
// 动机：Go 1.27 方法不能约束接收者已有的类型参数（spec「关键约束」第 4 条），
// Sum/Avg/Contains/Distinct/自然序 Sorted/Min/Max 等依赖元素约束的 API 在
// *Stream[T] 上只能以包级函数提供，无法链入管道。NumberStream 把约束收窄到
// 包装类型自有类型参数位：N 是 NumberStream 声明的类型参数，可带 Number 约束，
// 上述 API 得以回归方法形态——Range(0, 100).Sum() 一行闭环。
//
// 嵌入方式取舍：值嵌入 Stream[N]（与 Stream 嵌 pipeline 的全库风格一致）。
// 桥接语义为「复制句柄 + 立即标记原流消费」：AsNumber(s) 后 s 即作废、返回的
// NumberStream 持有独立句柄，二次桥接或继续使用 s 立即 panic（fail-fast），
// 与全部算子「链接即消费」的一次性契约一致；不做 *Stream 指针嵌入的惰性别名
// 语义（别名共享会让双重桥接静默放行，破坏一次性模型）。
//
// 方法面（19 个自有方法 + 提升）：元素保持型中间操作全部覆写为返回
// *NumberStream[N]（链不断窄）；未被覆写的提升方法保持 Stream 语义——类型迁移
// 算子（Map[U]/FlatMap 族/Scan/Zip 等）自然返回 *Stream（离开数值上下文是类型
// 事实），值终端（ToSlice/Count/First/Collect/Err 等）直接可用。被自然序版
// 遮蔽的比较器形态（Sorted/StableSorted/Min/Max 的 cmp 参数版）经 AsStream()
// 出口使用。

// NumberStream 是元素类型收窄为 Number 的流：在 Stream 全部能力之上，提供
// 依赖数值约束的方法形态终端（Sum/Avg/Min/Max/Contains）与自然序中间操作
// （Sorted/StableSorted/Distinct），链式调用不再逃逸到包级函数。
//
// 构造：Range（直接收窄）、OfNumber/FromNumberSlice、(*Stream[T]).MapToNumber、
// AsNumber 桥接。一次性消费语义与 Stream 完全一致。零值不可用（drive 为 nil），
// 一律经构造函数创建。
//
// 性能注记：元素保持型中间操作与构造入口相对等价 Stream 版每级多一次句柄
// 分配（构造期一次性，实测 ~65ns/112B，深度 4 纯构造链合计 +5 allocs/
// +560B/+~300ns，见 BenchmarkNumberStreamVsStream），求值热路径与 Stream
// 零差异（n=1e6 持平）。重构造轻求值的极端场景（每请求重建短链且元素极少）
// 可先以 *Stream 串联中间操作、末步 AsNumber 收窄后仅接终端。
type NumberStream[N Number] struct {
	Stream[N]
}

// wrapNumber 把上游操作产出的全新 *Stream[N] 句柄重新包装为数值流
// （链保持的实现基元）。fresh 须为未消费的新句柄（各算子构造路径保证）；
// 解引用拷贝后原指针即弃，不产生别名句柄。
func wrapNumber[N Number](fresh *Stream[N]) *NumberStream[N] {
	return &NumberStream[N]{*fresh}
}

// OfNumber 以可变参数构建数值流（Of 的收窄版）。
func OfNumber[N Number](xs ...N) *NumberStream[N] {
	return wrapNumber(Of(xs...))
}

// FromNumberSlice 基于 slice 构建数值流（FromSlice 的收窄版；零拷贝，
// 直接引用原切片，求值期间请勿并发修改）。
func FromNumberSlice[N Number](s []N) *NumberStream[N] {
	return wrapNumber(FromSlice(s))
}

// Range 构建整数区间数值流 [start, stop)（左闭右开，步长 1）。
// 直接返回 *NumberStream：区间元素必然是数值，就地收窄以解锁 Sum/Avg/Min/Max
// 等方法形态；需要 *Stream 时经 AsStream 桥接。
// （实现随构造函数自 construct.go 迁入，与其余收窄入口同置。）
func Range[I Integer](start, stop I) *NumberStream[I] {
	return wrapNumber(newHeadSplit(newRangeSp(start, stop, SpSized|SpOrdered)))
}

// AsNumber 把普通流桥接为数值流：收窄约束以解锁 Sum/Avg/Contains/Distinct
// 等方法形态。桥接即消费——原流 s 被立即标记一次性消费，返回的 NumberStream
// 持有独立句柄接管管道；二次桥接或继续使用 s 将 panic（fail-fast）。
// nil 流返回 nil（与包级便捷函数的容错对齐）。逆操作见 AsStream。
func AsNumber[N Number](s *Stream[N]) *NumberStream[N] {
	if s == nil {
		return nil
	}
	ns := &NumberStream[N]{*s}
	s.checkLinked()
	return ns
}

// AsStream 把数值流桥接回普通流：离开数值上下文的显式同型出口（消费本流）。
// 供 Zip 另一侧、Chunk/Enumerate 等包级函数与被遮蔽的比较器形态
// （Sorted/Min/Max 的 cmp 参数版）复用。返回的 *Stream 持有独立句柄接管管道，
// 本 NumberStream 随即作废。nil 接收者返回 nil。
func (s *NumberStream[N]) AsStream() *Stream[N] {
	if s == nil {
		return nil
	}
	ns := &Stream[N]{s.pipeline}
	s.checkLinked()
	return ns
}

// ---- 元素保持型中间操作（返回 *NumberStream[N]，链不断窄）----

// Filter 保留满足谓词 p 的元素。
func (s *NumberStream[N]) Filter(p func(N) bool) *NumberStream[N] {
	return wrapNumber(s.Stream.Filter(p))
}

// Peek 对每个元素施加副作用 f（不改变元素，常用于调试观察）。
// 并行流下 f 在分片 goroutine 内执行，观察顺序不保证（需保序请用 ForEach）。
func (s *NumberStream[N]) Peek(f func(N)) *NumberStream[N] {
	return wrapNumber(s.Stream.Peek(f))
}

// TakeWhile 保留首批满足 p 的元素，遇到首个不满足即终止（短路）。
func (s *NumberStream[N]) TakeWhile(p func(N) bool) *NumberStream[N] {
	return wrapNumber(s.Stream.TakeWhile(p))
}

// DropWhile 丢弃首批满足 p 的元素，之后全部放行。
func (s *NumberStream[N]) DropWhile(p func(N) bool) *NumberStream[N] {
	return wrapNumber(s.Stream.DropWhile(p))
}

// Limit 截取前 n 个元素（n == 0 得空流；无限源可借此终止；n < 0 panic）。
func (s *NumberStream[N]) Limit(n int64) *NumberStream[N] {
	return wrapNumber(s.Stream.Limit(n))
}

// Skip 跳过前 n 个元素，输出其余（n < 0 panic）。
// n == 0 恒等返回自身（不复制句柄、特征位与并行性透传，Task 14 语义）。
func (s *NumberStream[N]) Skip(n int64) *NumberStream[N] {
	if n == 0 {
		return s
	}
	return wrapNumber(s.Stream.Skip(n))
}

// Reverse 反转元素顺序。
func (s *NumberStream[N]) Reverse() *NumberStream[N] {
	return wrapNumber(s.Stream.Reverse())
}

// Sorted 依自然序（升序）排序，免写比较器（N 为 Number，是 cmp.Ordered
// 的子集）。不稳定（对齐 slices.SortFunc）。本方法遮蔽 Stream 的比较器版
// Sorted 与包级函数 Sorted[T cmp.Ordered]：自定义比较器经
// AsStream().Sorted(cmp)，普通 *Stream 用包级 Sorted。
func (s *NumberStream[N]) Sorted() *NumberStream[N] {
	return wrapNumber(s.Stream.Sorted(cmp.Compare[N]))
}

// StableSorted 依自然序（升序）稳定排序：等值元素保持相遇顺序（对齐
// slices.SortStableFunc；纯数值等值即全等，结果与 Sorted 一致，语义上为
// 需要稳定性的场景预留）。遮蔽 Stream 的比较器版 StableSorted；
// Stream 侧同目的形态为 s.Stream.StableSorted(cmp)（无包级稳定排序便捷函数）。
func (s *NumberStream[N]) StableSorted() *NumberStream[N] {
	return wrapNumber(s.Stream.StableSorted(cmp.Compare[N]))
}

// Distinct 依据元素自身去重（保留首见，保持遇序；N 为 Number，全部可比较）。
// 浮点 NaN 互不相等，各 NaN 均保留（同 map 键语义）。本方法为 NumberStream
// 新增形态；按键去重（键函数任意）用提升的 DistinctBy[K comparable]。
func (s *NumberStream[N]) Distinct() *NumberStream[N] {
	return wrapNumber(s.Stream.DistinctBy(func(v N) N { return v }))
}

// ---- 标志与生命周期（返回 *NumberStream[N]，链不断窄）----

// Parallel 声明后续求值以最多 n 个分片并行（n <= 1 或不可分源自动串行）。
func (s *NumberStream[N]) Parallel(n int) *NumberStream[N] {
	return wrapNumber(s.Stream.Parallel(n))
}

// Sequential 还原串行求值（抵消上游 Parallel 声明）。
func (s *NumberStream[N]) Sequential() *NumberStream[N] {
	return wrapNumber(s.Stream.Sequential())
}

// Unordered 声明后续求值不依赖相遇顺序（并行求值按分片完成序流式合并）。
func (s *NumberStream[N]) Unordered() *NumberStream[N] {
	return wrapNumber(s.Stream.Unordered())
}

// OnClose 注册资源清理回调 f：求值结束自动触发一次（幂等，按注册序，
// 出错记首错可经 Err() 查询）。
func (s *NumberStream[N]) OnClose(f func() error) *NumberStream[N] {
	return wrapNumber(s.Stream.OnClose(f))
}

// ---- 收窄红利终端（元素约束下的方法形态；委托包级实现，零逻辑重复）----

// Sum 数值求和（空流返回 0）。包级 Sum 的方法形态，串行求值语义一致；
// 并行数值聚合用提升的 Reduce(0, add)（片内折叠、片序合并）。
func (s *NumberStream[N]) Sum() N {
	return Sum(&s.Stream)
}

// Avg 数值平均（空流返回 0；整数类型按整除）。包级 Avg 的方法形态。
func (s *NumberStream[N]) Avg() N {
	return Avg(&s.Stream)
}

// Min 依自然序取最小（空流返回零值与 false）。免写比较器形态；本方法
// 遮蔽 Stream 的比较器版 Min（自定义比较器经 AsStream().Min(cmp)），
// 普通 *Stream 的同目的形态为包级函数 Min。
func (s *NumberStream[N]) Min() (N, bool) {
	return Min(&s.Stream)
}

// Max 依自然序取最大（空流返回零值与 false）。免写比较器形态；本方法
// 遮蔽 Stream 的比较器版 Max（自定义比较器经 AsStream().Max(cmp)），
// 普通 *Stream 的同目的形态为包级函数 Max。
func (s *NumberStream[N]) Max() (N, bool) {
	return Max(&s.Stream)
}

// Contains 判断流中是否含有目标元素（包级 Contains 的方法形态；
// 短路：命中即停止遍历）。N 为 Number（comparable 子集），== 比较在此合法。
func (s *NumberStream[N]) Contains(target N) bool {
	return Contains(&s.Stream, target)
}

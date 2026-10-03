package stream

import "time"

// time_window.go：时间窗口分桶（tumbling window，独立于既有算子文件）
// 及其配套算子（SortedByTime 时间序 / CompleteTimeBuckets 空桶补全）。
//
// 与 WindowSliding（逐元素滑动的定长窗口）/Chunk（定长计数分组）互补：
// 按固定时间间隔而非固定元素个数分窗，对标 Julia（TimeSeries resample）
// /Scala（Akka groupedWithin、Spark 翻转窗口）的时间窗口能力。
//
// 配套算子遵循「便利以显式组合提供，不替用户做决定」的决议：原始采样
// 数据通常已时间升序（TimeWindow 输出即时间序的常见路径，零额外开销），
// 不为低频乱序场景增加默认排序；乱序上游的时间序需求由 SortedByTime
// 显式表达，空桶补全由 CompleteTimeBuckets 显式表达（主流对照：内核均
// 为 time_bucket+GroupBy，桶序惯例为时间升序——升序数据是常见路径，
// 乱序需求经配套算子显式化；空桶主流默认不含，pandas resample 含空桶
// 为少数派）。
//
// 独立成文件且不依赖 newStateful 的类型迁移泛化（既有签名为同型
// T→T）：此处内联「物化 → 变换回放」两段式协议，与 newStateful 语义
// 一致——上游出错（错误即值）时变换不执行、Begin(0)/End 仍配对。

// TimeBucket 是 TimeWindow 产出的时间窗口桶：Start 为桶的对齐起始时间
// （桶内元素时间戳以窗口宽度 Truncate 后的共同值），Items 为桶内元素
// （保持相遇序）。
type TimeBucket[T any] struct {
	Start time.Time
	Items []T
}

// TimeWindow 时间窗口分桶：以 ts(v).Truncate(d) 为桶键，把元素分入
// 对齐时间网格的互不重叠的翻转窗口（tumbling window）——固定时间
// 间隔而非固定元素个数。
//
// 语义（time.Truncate 桶化 + GroupBy）：桶输出顺序为桶键首现序、桶内
// 保持相遇序（对齐 collector.GroupingBy 保遇序）；乱序/晚到的元素并入
// 其桶键对应的既有桶（桶不拆分）；不产空桶（无元素的窗口不存在，
// 需要补空窗/补零时由调用方后处理）。上游时间乱序时输出为键首现序
// 而非时间序——需要时间序时接配套算子 SortedByTime。桶键以
// ts(v).Truncate(d).UTC() 统一规范化：同一瞬间恒落同一桶（time.Time
// 作 map 键含 Location 判等，混合时区表示的等值瞬间否则会被拆桶），
// Start 恒为 UTC 网格点——需要本地时区网格或展示时，由调用方对 Start
// 做 .In(loc) 后处理。
//
// 桶级聚合（重采样）由 Map 组合表达，不设独立聚合入口：
//
//	TimeWindow(s, ts, d).Map(func(b TimeBucket[T]) R { ... })
//	// 如每桶计数：FromSlice(b.Items).Count()；
//	// 如每桶求和/均值：对 b.Items 内联聚合或交 collector 处理。
//
// 物化型有状态（两段式分段求值）→ 并行降级（splitN 不继承）、不支持
// 无限源（配合 Limit 先行截断可用）；物化输出已知有限（置 SpLimited，
// 可作 Join/LeftJoin 右流）。上游出错（错误即值）时不产出任何
// 桶，Err() 可查。ts 为 nil 或 d <= 0 panic；nil 流返回 nil。
//
// 包级函数形态：方法返回 Stream[TimeBucket[T]]（T 的派生类型——
// TimeBucket[T] 含 Items []T）触发 Go 1.27 实例化循环
// （T → TimeBucket[T] → TimeBucket[TimeBucket[T]] → …，实测复现），
// 同 Chunk/WindowSliding 之因。
func TimeWindow[T any](s *Stream[T], ts func(T) time.Time, d time.Duration) *Stream[TimeBucket[T]] {
	if s == nil {
		return nil
	}
	if ts == nil {
		panic("stream: TimeWindow time function is nil")
	}
	if d <= 0 {
		panic("stream: TimeWindow window width must be positive")
	}
	s.checkLinked()
	driveUpstream := s.drive
	return &Stream[TimeBucket[T]]{pipeline[TimeBucket[T]]{
		drive: func(down Sink[TimeBucket[T]], ec *evalCtx) {
			cs := &collectingSink[T]{limit: -1}
			driveUpstream(cs, ec) // 第一段：驱动上游物化，collectingSink 为该段终端
			var out []TimeBucket[T]
			if ec.firstErr() == nil {
				idx := make(map[time.Time]int, len(cs.buf)) // 桶键 → out 下标（首现序）
				for _, v := range cs.buf {
					// Truncate 以绝对时间网格对齐（同一瞬间的不同 Location 表示
					// 截断后仍为等值瞬间），但 time.Time 作 map 键按结构体 ==
					// （含 Location 指针）判等，混合 Location 的数据会把同一
					// 瞬间拆成两个桶——统一 .UTC() 规范化键表示。Start 因此恒为
					// UTC 网格点；需要本地时区展示时由调用方对 Start 做 .In(loc)。
					k := ts(v).Truncate(d).UTC()
					i, ok := idx[k]
					if !ok { // 新桶：按键首现序追加
						i = len(out)
						idx[k] = i
						out = append(out, TimeBucket[T]{Start: k})
					}
					out[i].Items = append(out[i].Items, v) // 晚到并入既有桶，桶内保遇序
				}
			}
			down.Begin(int64(len(out))) // 第二段：单遍回放
			for _, b := range out {
				if !down.Accept(b) {
					break
				}
			}
			down.End()
		},
		// 物化型统一规则：强制置 SpLimited（求值能完成即输出有限）；
		// 置 SpSized/SpSubSized（桶数物化后已知）、清 SpSorted/SpDistinct。
		chars:   (s.chars | SpSized | SpSubSized | SpLimited) &^ (SpSorted | SpDistinct),
		parN:    s.parN, // 并行标志保留但 splitN 不继承（零值 nil），求值自动串行
		closers: s.closers,
	}}
}

// SortedByTime 把 TimeBucket 流按 Start 时间升序排序：TimeWindow 输出的
// 免比较器配套形态（方法 Sorted 需自写比较器；自然序包级 Sorted 要求
// cmp.Ordered，结构体不满足），避免为 TimeBucket 手写比较器。
//
// TimeWindow 的桶序为键首现序：上游时间升序时输出即时间序（常见路径
// 零额外开销）；上游时间乱序时输出非时间序——需要时间序的重采样场景
// 请接本算子。桶键唯一（同一网格起点仅一桶），等键元素不存在，稳定性
// 无差异。nil 流返回 nil。
//
// 包级函数形态：方法无法把接收者约束为 Stream[TimeBucket[T]] 形态
// （Go 1.27 泛型方法硬限制的接收者形态约束表现）。
func SortedByTime[T any](s *Stream[TimeBucket[T]]) *Stream[TimeBucket[T]] {
	if s == nil {
		return nil
	}
	return s.Sorted(func(a, b TimeBucket[T]) int { return a.Start.Compare(b.Start) })
}

// maxTimeBuckets 是 CompleteTimeBuckets 的溢出护栏：补全桶数上限
// （防首末桶跨度/宽度失配的天量分配）。
const maxTimeBuckets = 1 << 20 // 1<<20 ≈ 一百万桶（覆盖秒级网格×数年）

// CompleteTimeBuckets 补全时间窗口空桶：对已按时间升序的 TimeBucket 流，
// 在相邻桶 Start 间隔超过 d 的空档内按 d 步进插入空桶（Items 为 nil），
// 输出保持时间升序。宽度 d 为窗口宽度（与 TimeWindow 的 d 一致，不传
// 对则填不对网格）。范围数据驱动：首桶之前与末桶之后的空窗不补（起始
// 时刻属调用方域知识——从零时刻补起几乎必非所愿）。
//
// 结构补全与填值分离（对齐 pandas resample+fillna / Polars upsample+
// fill_null 的分工）：本算子只补桶不填值；需要补零/前值等填值语义时由
// 后接算子组合表达。
//
// 输入必须升序：本算子不排序（乱序输入语义未定义），需时间序先接
// SortedByTime。溢出护栏 maxTimeBuckets：补全桶数超限时 panic，防御
// 首末桶时间差/宽度失配（如误传毫秒宽给跨月数据）导致的天量内存。
// d <= 0 panic；nil 流返回 nil。
//
// 包级函数形态（实测复现）：接收者形态 Stream[TimeBucket[T]] 无法声明
// （receiver type parameter must be an identifier），同 TimeWindow 之因。
func CompleteTimeBuckets[T any](s *Stream[TimeBucket[T]], d time.Duration) *Stream[TimeBucket[T]] {
	if s == nil {
		return nil
	}
	if d <= 0 {
		panic("stream: CompleteTimeBuckets window width must be positive")
	}
	return newStateful(s, -1, func(buf []TimeBucket[T]) []TimeBucket[T] {
		if len(buf) == 0 {
			return buf
		}
		var out []TimeBucket[T]
		cur := buf[0].Start
		for _, b := range buf {
			for b.Start.After(cur) { // 间隙：网格点早于当前桶时插空桶
				if len(out) > maxTimeBuckets {
					panic("stream: CompleteTimeBuckets number of buckets to backfill exceeds limit " +
						"(check that width d matches the time scale of the data)")
				}
				out = append(out, TimeBucket[T]{Start: cur})
				cur = cur.Add(d)
			}
			// 空档非 d 整数倍（桶 Start 在网格点之间）时网格点已越过桶：
			// 追过即停，直接产出原桶并以其 Start 重锚网格（不丢桶、不补出负间隔）。
			out = append(out, b)
			cur = b.Start.Add(d)
		}
		return out
	}, s.chars|SpSized|SpSubSized|SpLimited)
}

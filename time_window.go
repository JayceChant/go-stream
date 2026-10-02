package stream

import "time"

// time_window.go：时间窗口分桶（tumbling window，独立于既有算子文件）。
//
// 与 WindowSliding（逐元素滑动的定长窗口）/Chunk（定长计数分组）互补：
// 按固定时间间隔而非固定元素个数分窗，对标 Julia（TimeSeries resample）
// /Scala（Akka groupedWithin、Spark 翻转窗口）的时间窗口能力。
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
// 需要补空窗/补零时由调用方后处理）。
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
		panic("stream: TimeWindow 时间函数为 nil")
	}
	if d <= 0 {
		panic("stream: TimeWindow 窗口宽度必须为正")
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
					k := ts(v).Truncate(d)
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

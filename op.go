package stream

// op.go 提供管道 stage 的构造函数（无状态 / 有状态物化型）与一次性消费检查，
// 是公开算子方法接入求值引擎的唯一入口。
//
// Begin/End 协议约定：每一段（源段或物化后的续段）各自保证
// 「Begin 恒先于推送、End 恒在段末（含出错与短路路径）」，
// 终端 sink 因此总能观察到配对完整的 Begin/End。

// errConsumed 是违反一次性消费语义时的 panic 文案（编程错误，不可恢复）。
const errConsumed = "stream: 该流已被链接或消费，不可重复使用（一次性语义）"

// checkLinked 检查本 stage 是否已被链接或消费：是则 panic，否则置位 consumed。
func (p *pipeline[T]) checkLinked() {
	if p.consumed {
		panic(errConsumed)
	}
	p.consumed = true
}

// newStateless 追加一个无状态阶段：仅组合求值闭包，不触发任何遍历（惰性）。
//
// 双类型参数 T→U 支持元素类型迁移的算子（Map/FlatMap 等）；同型算子
// 调用时 U 自动推断为 T。wrap 把下游 sink 包装成本阶段的 sink
// （Java opWrapSink 的等价物，求值时与全部无状态算子融合为单遍）；
// 返回的新流以本流为上游，本流随即标记 consumed（不可再被链接）。
// 并行标志与分片闭包原样继承（splitN 为擦除形态，可穿越 T→U 异构边界）。
func newStateless[T, U any](
	up *Stream[T],
	wrap func(down Sink[U], ec *evalCtx) Sink[T],
	chars Characteristics,
) *Stream[U] {
	up.checkLinked()
	ud := up.drive
	return &Stream[U]{pipeline[U]{
		drive: func(down Sink[U], ec *evalCtx) {
			ud(wrap(down, ec), ec)
		},
		chars:   chars,
		parN:    up.parN,
		splitN:  up.splitN,
		closers: up.closers,
	}}
}

// collectingSink 是有状态阶段第一段（源段）的终端：把元素物化进缓冲。
// limit >= 0 时收满即请求取消，支持无限源的短路收集（Limit）。
type collectingSink[T any] struct {
	buf   []T
	limit int64 // -1 表示不限
}

func (c *collectingSink[T]) Begin(size int64) {
	if size > 0 {
		n := size
		if c.limit >= 0 && c.limit < n {
			n = c.limit
		}
		c.buf = make([]T, 0, n)
	}
}

func (c *collectingSink[T]) Accept(t T) bool {
	if c.limit >= 0 && int64(len(c.buf)) >= c.limit {
		return false // 已达收集上限：拒绝并请求取消（含 limit=0）
	}
	c.buf = append(c.buf, t)
	return true
}

func (c *collectingSink[T]) End() {}

// newStateful 追加一个有状态（物化型）阶段，分段求值：
//
// 第一段驱动上游把元素收集进临时缓冲（limit 可提前截断，支持无限源）；
// 第二段以 process 变换缓冲（排序/去重/跳过等纯切片变换）后单遍推入下游。
//
// 与 spec 最初设想的「materialize + wrap 双闭包」相比，此处收敛为
// 「limit + process」两点式物化策略：续段的 Begin/End/短路协议由引擎统一
// 处理，算子只描述切片变换，不易出错；该签名同时为后续并行分片预留。
//
// 并行求值在此降级（splitN 置 nil）：物化需要全量元素，分片物化 +
// 分片变换的语义组合复杂度远超 v1 目标（如 Skip 的跨片偏移）。
func newStateful[T any](
	up *Stream[T],
	limit int64,
	process func(buf []T) []T,
	chars Characteristics,
) *Stream[T] {
	up.checkLinked()
	ud := up.drive
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) {
			cs := &collectingSink[T]{limit: limit}
			ud(cs, ec) // 第一段：物化，collectingSink 为该段终端
			var out []T
			if ec.firstErr() == nil {
				out = process(cs.buf)
			}
			down.Begin(int64(len(out))) // 第二段：单遍回放
			for _, v := range out {
				if !down.Accept(v) {
					break
				}
			}
			down.End()
		},
		chars:   chars,
		parN:    up.parN, // 并行标志保留但 splitN 已降级，求值自动串行
		closers: up.closers,
	}}
}

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
// wrap 把下游 sink 包装成本阶段的 sink（Java opWrapSink 的等价物，求值时
// 与全部无状态算子融合为单遍）；返回的新流以本流为上游，本流随即标记
// consumed（不可再被其它中间操作链接）。
func newStateless[T any](
	up *Stream[T],
	wrap func(down Sink[T], ec *evalCtx) Sink[T],
	chars Characteristics,
) *Stream[T] {
	up.checkLinked()
	ud := up.drive
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) {
			ud(wrap(down, ec), ec)
		},
		chars: chars,
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
	c.buf = append(c.buf, t)
	return c.limit < 0 || int64(len(c.buf)) < c.limit
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
			if ec.err == nil {
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
		chars: chars,
	}}
}

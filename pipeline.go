package stream

import (
	"sync"
	"sync/atomic"
)

// evalCtx 是一次终止求值的共享上下文（错误即值模型）：
// 由终止求值入口创建，沿 drive 链层层传递，记录求值过程中出现的首个可预期错误；
// 求值结束后由发起终止操作的 Stream 实例读取（经 Err() 暴露给用户）。
//
// 并发安全：err 的写入以 mutex 保护——Zip 等双流算子在求值内并发驱动两条
// 管道（各自携带本 ctx），首错保留语义需原子化；读路径（fail 后短路分支、
// 求值结束写回）均为同步调用，无需额外同步。
type evalCtx struct {
	mu       sync.Mutex
	err      error
	panicVal any // 后台 goroutine（如 Zip）中用户回调 panic 的暂存值
	// partSrc 为并行求值的分片源覆盖（类型擦除，仅 Head 段求值闭包读取；
	// 空间上安全：分片由 splitN 闭包产生，类型与 Head 元素型一致）。
	partSrc any
	// cancel 为并行求值的短路广播：终端请求取消后各分片提前停止遍历。
	cancel atomic.Bool
}

// fail 记录首个错误并返回 false，供 sink 以 return ec.fail(err)
// 在一条语句内同时表达「记录错误」与「请求短路」。
func (ec *evalCtx) fail(err error) bool {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if ec.err == nil {
		ec.err = err
	}
	return false
}

// firstErr 返回已记录的首错（并发读安全）。
func (ec *evalCtx) firstErr() error {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.err
}

// takePanic 取出后台 goroutine 暂存的 panic 值（一次性读取）。
func (ec *evalCtx) takePanic() any {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	p := ec.panicVal
	ec.panicVal = nil
	return p
}

// pipeline 是流的核心求值机，被 Stream 嵌入组合（Java AbstractPipeline 的 Go 化）。
//
// 与 Java 的链表式 AbstractPipeline 不同：Go 无 raw type，无法把异构元素类型的
// 上游（如 Map 阶段前 T 后 U）存入同型 upstream 字段，因此采用闭包组合——
// 每个 stage 在构造时把上游引用与 wrap 包装闭包捕获进自身的 drive 求值闭包。
// drive 的语义：驱动「源 → … → 上游 → 本 stage」整段，把本 stage 的输出推入 down。
type pipeline[T any] struct {
	// drive 为本 stage 的求值闭包；Head stage 的 drive 为遍历 source 推入 down。
	// nil 仅出现在尚未接入引擎的中间状态，正常构造路径不会暴露给用户。
	drive func(down Sink[T], ec *evalCtx)
	// source 为数据源，仅 Head stage 非空：供特征位查询与并行求值分片。
	source Splitterator[T]
	// chars 为本 stage 的输出特征位（沿管道由各算子按传播规则改写）。
	chars Characteristics
	// consumed 为一次性消费标志：被下游中间操作链接或被终止求值后置位；
	// 再次使用将 panic（编程错误，不可恢复）。
	consumed bool
	// err 保存最近一次由本实例发起的终止求值的首错，供 Err() 读取。
	err error
	// parN 为本 stage 起生效的并行度：0 串行；>0 表示后续求值最多 n 个分片
	// 并行（实际并行还受可分源与降级规则约束，见 parallel.go）。
	parN int
	// splitN 为类型擦除的分片闭包（并行求值用）：返回最多 n 个子源
	// （各为 Head 元素型的 Splitterator，以 any 擦除传递）。仅可分源
	// （slice/range）的 Head 构造时设置，沿链原样传播——擦除形态可穿越
	// Map 等异构 stage（Go 无 raw type，异构上游无法以同型字段存放）；
	// Head 段求值闭包经 ec.partSrc 断言恢复具体类型（构造拓扑保证一致）。
	// 物化型有状态算子与双流算子将其置 nil（降级点）。
	splitN func(n int) []any
}

// newHead 构造持有数据源的 Head stage（流的起点）。
// splitN（分片闭包）不在此设置：TrySplit 探测有消耗副作用，可分性由
// 构造函数（construct.go）依据源类型显式指定，见 splitNOf。
func newHead[T any](src Splitterator[T]) *Stream[T] {
	return &Stream[T]{pipeline[T]{
		drive:  driveFromSource(src),
		source: src,
		chars:  src.Characteristics(),
	}}
}

// splitNOf 包装一个源为分片闭包（不立即分片；分片发生在求值期，
// 届时 TrySplit 的消耗语义正是所需）。不可分源传入也能构造——
// splitSrc 首次 TrySplit 返回 nil 时产生 nil，调用方据此降级串行。
func splitNOf[T any](src Splitterator[T]) func(n int) []any {
	return func(n int) []any { return splitSrc(src, n) }
}

// splitSrc 递归二分源为（最多）n 个子源，返回按相遇序排列的子源切片
// （nil 表示不可分或无需分）。TrySplit 语义：返回后半段、自身收缩为前半段。
func splitSrc[T any](src Splitterator[T], n int) []any {
	if n < 2 {
		return nil
	}
	back := src.TrySplit()
	if back == nil {
		return nil
	}
	var out []any
	if n == 2 {
		out = append(out, src, back)
		return out
	}
	// 前半段继续二分至 n/2 份，后半段至 n-n/2 份（保序拼接）
	for _, part := range splitSrc(src, n/2) {
		out = append(out, part)
	}
	out = append(out, splitSrc(back, n-n/2)...)
	return out
}

// driveFromSource 把数据源编译为 Head 段的求值闭包：单遍推动源推入 down，
// 短路（Accept 返回 false）时立即停止遍历，End 恒在段末调用。
// 求值时若 ec.partSrc 携带分片源（并行求值），以分片源替代原源遍历。
// 源侧的可预期错误（FromFunc）由 construct.go 的专属 head 路径处理，不经此函数。
func driveFromSource[T any](src Splitterator[T]) func(down Sink[T], ec *evalCtx) {
	return func(down Sink[T], ec *evalCtx) {
		cur := src
		if part, ok := ec.partSrc.(Splitterator[T]); ok && part != nil {
			cur = part
		}
		down.Begin(cur.EstimateSize())
		cur.ForEachRemaining(func(t T) bool {
			if ec.cancel.Load() {
				return false
			}
			return down.Accept(t)
		})
		down.End()
	}
}

// evaluate 是全部终止操作的求值入口：一次性消费检查、创建求值上下文、
// 执行 drive 链、错误写回本实例（供 Err() 读取）。
// 参数 down 为终止 sink；返回 ec 以便调用方决定是否因错误提前退出。
// parTotal 非 nil 时为并行求值路径（分片/降级/合并见 parallel.go）。
func (p *pipeline[T]) evaluate(down Sink[T]) *evalCtx {
	return p.evaluateNP(down, nil)
}

// evaluateNP 为 evaluate 的完整形态：parTotal 非 nil 且满足并行条件时
// 走并行分片求值，否则串行（down 直驱）。
func (p *pipeline[T]) evaluateNP(down Sink[T], parTotal parallelTotal[T]) *evalCtx {
	p.checkLinked()
	ec := &evalCtx{}
	if parTotal != nil && p.parN > 1 && p.splitN != nil {
		evaluateParallel(p, down, ec, parTotal)
	} else {
		p.drive(down, ec)
	}
	p.err = ec.firstErr()
	return ec
}

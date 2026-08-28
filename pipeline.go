package stream

import "sync"

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
	// source 为数据源，仅 Head stage 非空：供特征位查询与后续并行拆分（TODO）预留。
	source Splitterator[T]
	// chars 为本 stage 的输出特征位（沿管道由各算子按传播规则改写）。
	chars Characteristics
	// consumed 为一次性消费标志：被下游中间操作链接或被终止求值后置位；
	// 再次使用将 panic（编程错误，不可恢复）。
	consumed bool
	// err 保存最近一次由本实例发起的终止求值的首错，供 Err() 读取。
	err error
}

// newHead 构造持有数据源的 Head stage（流的起点）。
func newHead[T any](src Splitterator[T]) *Stream[T] {
	return &Stream[T]{pipeline[T]{
		drive:  driveFromSource(src),
		source: src,
		chars:  src.Characteristics(),
	}}
}

// driveFromSource 把数据源编译为 Head 段的求值闭包：单遍推动源推入 down，
// 短路（Accept 返回 false）时立即停止遍历，End 恒在段末调用。
// 源侧的可预期错误（FromFunc）由 Task 3 的专属 head 路径处理，不经此函数。
func driveFromSource[T any](src Splitterator[T]) func(down Sink[T], ec *evalCtx) {
	return func(down Sink[T], ec *evalCtx) {
		down.Begin(src.EstimateSize())
		src.ForEachRemaining(func(t T) bool {
			return down.Accept(t)
		})
		down.End()
	}
}

// evaluate 是全部终止操作的求值入口：一次性消费检查、创建求值上下文、
// 执行 drive 链、错误写回本实例（供 Err() 读取）。
// 参数 down 为终止 sink；返回 ec 以便调用方决定是否因错误提前退出。
func (p *pipeline[T]) evaluate(down Sink[T]) *evalCtx {
	p.checkLinked()
	ec := &evalCtx{}
	p.drive(down, ec)
	p.err = ec.firstErr()
	return ec
}

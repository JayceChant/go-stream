package stream

// evalCtx 是一次终止求值的共享上下文（错误即值模型）：
// 由终止求值入口创建，沿 drive 链层层传递，记录求值过程中出现的首个可预期错误；
// 求值结束后由发起终止操作的 Stream 实例读取（经 Err() 暴露给用户）。
type evalCtx struct {
	err error
}

// fail 记录首个错误并返回 false，供 sink 以 return ec.fail(err)
// 在一条语句内同时表达「记录错误」与「请求短路」。
func (ec *evalCtx) fail(err error) bool {
	if ec.err == nil {
		ec.err = err
	}
	return false
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

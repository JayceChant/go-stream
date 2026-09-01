package stream

import "sync"

// parallel.go：并行求值 v1（Parallel(n)/Sequential/Unordered）。
//
// 设计要点（详见 spec「并行求值 v1」与「生命周期与可重放」）：
//   - 分片：p.splitN（类型擦除闭包）在求值期递归 TrySplit 至 n 份（保序）
//   - 求值：每片 goroutine 独立重入 p.drive（经 ec.partSrc 覆盖 Head 源），
//     每片全新 sink 链 + 独立终端累积（无共享可变状态，-race 安全）
//   - 合并：有序流按分片序物化回放进用户终端（保序）；无序流
//     （SpOrdered 缺失）且终端支持时按完成序流式并入（先完成先推，
//     不等待全部片，降低端到端延迟）
//   - 降级：短路终止族 / 物化型有状态算子之后（splitN=nil）/
//     不可分源（splitN 返回 nil）→ 串行（正确性优先）
//   - 错误：片内首错按片序合并进主槽；片内回调 panic 由发起 goroutine re-panic

// Parallel 声明后续求值以最多 n 个分片并行（中间操作语义：消费上游，
// 返回携带并行标志的新流）。n <= 1 或不可分源/含降级算子的管道自动串行。
func (s *Stream[T]) Parallel(n int) *Stream[T] {
	if n < 1 {
		n = 1
	}
	return s.newFlagStage(n)
}

// Sequential 还原串行求值（抵消上游 Parallel 声明）。
func (s *Stream[T]) Sequential() *Stream[T] {
	return s.newFlagStage(0)
}

// Unordered 声明后续求值不依赖相遇顺序（清除 SpOrdered 特征位，纯标志
// stage，不改变元素流与并行度）。对应 Java BaseStream.unordered()。
//
// 语义效果：并行求值下分片结果按完成序流式并入终端（先完成先推），
// 降低端到端延迟；结果集合与串行一致，顺序不保证（本就是无序语义）。
// 仅 ToSlice/ForEach/Min/Max（元素级）与 Collect（Combiner 按完成序合并）
// 参与流式合并；Count/Reduce 仍按片序聚合（结果不受影响）。
func (s *Stream[T]) Unordered() *Stream[T] {
	ns := s.newFlagStage(s.parN)
	ns.chars &^= SpOrdered
	return ns
}

// newFlagStage 追加一个纯标志 stage：不改变元素流，仅改写并行度。
// splitN、chars 与清理回调链原样继承（splitN 为 nil 时并行标志无效果，
// 即自动降级）。
func (s *Stream[T]) newFlagStage(n int) *Stream[T] {
	s.checkLinked()
	driveUpstream := s.drive
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) { driveUpstream(down, ec) },
		chars: s.chars,
		parN:  n,
		splitN: func(k int) []any {
			if s.splitN == nil {
				return nil
			}
			return s.splitN(k)
		},
		closers: s.closers,
	}}
}

// parallelTotal 是并行求值的终端协议：由参与并行的终止操作实现。
// part 每片调用一次（片内 goroutine）；total 在全部片完成后按分片序
// 于发起 goroutine 调用一次。二者共同保证"每片独立累积、串行合并"。
type parallelTotal[T any] interface {
	// part 返回该片使用的终端 sink（每片全新实例，独立累积）。
	part() Sink[T]
	// total 合并全部片（parts 与片序一致）：按序把各片结果推入用户终端 down。
	total(parts []Sink[T], down Sink[T], ec *evalCtx)
}

// streamTotal 是并行终端的无序流式合并扩展（可选实现）：支持分片
// 先完成先推。仅无序流（SpOrdered 缺失）时启用。
type streamTotal[T any] interface {
	parallelTotal[T]
	// pushPart 把已完成分片 i 的结果立即推入用户终端 down（完成序调用，
	// 发起 goroutine 内串行执行）。down.Begin 已由引擎以总量未知（-1）
	// 先行调用，End 由引擎在全部片处理完后统一收尾。
	// 返回 false 表示用户终端请求取消：引擎停止后续分片的推送。
	pushPart(i int, sinks []Sink[T], down Sink[T], ec *evalCtx) bool
}

// evaluateParallel 执行并行分片求值：
// 分片 → 串行预创建各片终端（无竞争）→ 各片 goroutine 独立求值 →
// 有序流按片序 total 合并 / 无序流先完成先推（流式合并）→ 用户终端。
func evaluateParallel[T any](p *pipeline[T], down Sink[T], ec *evalCtx, pt parallelTotal[T]) {
	parts := p.splitN(p.parN)
	if len(parts) < 2 {
		p.drive(down, ec) // 不可分（或单份）：降级串行
		return
	}
	// 无序流且终端支持流式合并：先完成先推，不等待全部片。
	if p.chars&SpOrdered == 0 {
		if st, ok := pt.(streamTotal[T]); ok {
			evaluateParallelStream(p, down, ec, parts, st)
			return
		}
	}
	sinks := make([]Sink[T], len(parts))
	for i := range sinks {
		sinks[i] = pt.part() // 主 goroutine 串行创建：片级状态登记安全
	}
	if vals := runParts(p, sinks, parts, ec, nil)(); len(vals) > 0 {
		panic(vals[0]) // 按捕获序 re-panic（近似首片 panic）
	}
	pt.total(sinks, down, ec) // 分片序合并
}

// evaluateParallelStream 无序流式合并：每片完成即把该片结果推入用户
// 终端（完成序），无需等待全部片完成。总量未知 → down.Begin(-1)；
// 全部片结束后 down.End() 收尾，panic 值按捕获序 re-panic。
func evaluateParallelStream[T any](p *pipeline[T], down Sink[T], ec *evalCtx, parts []any, st streamTotal[T]) {
	sinks := make([]Sink[T], len(parts))
	for i := range sinks {
		sinks[i] = st.part() // 主 goroutine 串行创建：片级状态登记安全
	}
	done := make(chan int, len(parts)) // 缓冲充足：片 goroutine 发送不阻塞
	var panics []any
	go func() {
		panics = runParts(p, sinks, parts, ec, done)()
		close(done)
	}()
	down.Begin(-1)
	cancelled := false
	for i := range done { // 片完成即推；排空至 close 保证 panics 读取前 happens-before
		if !cancelled && !st.pushPart(i, sinks, down, ec) {
			cancelled = true // 用户终端取消：不再推送（在途分片仍等待结束）
		}
	}
	down.End()
	if len(panics) > 0 {
		panic(panics[0])
	}
}

// runParts 并行驱动各分片：每片 goroutine 独立重入 p.drive（ec.partSrc
// 覆盖源、全新 sink 链），片内首错并入主错误槽；片内回调 panic 捕获
// 暂存。done 非 nil 时片完成即投递片下标（容量须 ≥ 片数，发送不阻塞）。
// 返回收割函数：阻塞至全部片结束，返回捕获的 panic 值（按捕获序）。
func runParts[T any](p *pipeline[T], sinks []Sink[T], parts []any, ec *evalCtx, done chan<- int) func() []any {
	var panicMu sync.Mutex
	var panicVals []any
	var wg sync.WaitGroup
	wg.Add(len(parts))
	for i := range parts {
		go func(i int, src any) {
			defer wg.Done()
			defer func() {
				if pv := recover(); pv != nil {
					panicMu.Lock()
					panicVals = append(panicVals, pv)
					panicMu.Unlock()
				}
			}()
			pec := &evalCtx{partSrc: src}
			p.drive(sinks[i], pec) // 独立重入：全新 sink 链
			pec.mu.Lock()
			if pec.err != nil {
				ec.fail(pec.err) // 片内首错并入主槽（并发安全）
			}
			pec.mu.Unlock()
			if done != nil {
				done <- i
			}
		}(i, parts[i])
	}
	return func() []any {
		wg.Wait()
		panicMu.Lock()
		defer panicMu.Unlock()
		return panicVals
	}
}

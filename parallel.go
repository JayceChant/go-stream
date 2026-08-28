package stream

import "sync"

// parallel.go：并行求值 v1（Parallel(n)/Sequential）。
//
// 设计要点（详见 spec「并行求值 v1」）：
//   - 分片：p.splitN（类型擦除闭包）在求值期递归 TrySplit 至 n 份（保序）
//   - 求值：每片 goroutine 独立重入 p.drive（经 ec.partSrc 覆盖 Head 源），
//     每片全新 sink 链 + 独立终端累积（无共享可变状态，-race 安全）
//   - 合并：物化各片结果后按分片序回放进用户终端（Ordered 保序）；
//     Collect 专属路径按分片序 Combiner 合并
//   - 降级：短路终止族 / 物化型有状态算子之后（splitN=nil）/
//     不可分源（splitN 返回 nil）→ 串行（正确性优先）
//   - 错误：片内首错按片序合并进主槽；片内回调 panic 由发起 goroutine re-panic

// Parallel 声明后续求值以最多 n 个分片并行（中间操作语义：消费上游，
// 返回携带并行标志的新流）。n <= 1 或不可分源/含降级算子的管道自动串行。
func (s *Stream[T]) Parallel(n int) *Stream[T] {
	if n < 1 {
		n = 1
	}
	return s.newParStage(n)
}

// Sequential 还原串行求值（抵消上游 Parallel 声明）。
func (s *Stream[T]) Sequential() *Stream[T] {
	return s.newParStage(0)
}

// newParStage 追加一个纯标志 stage：不改变元素流与特征位，仅改写并行度。
// splitN 与 chars 原样继承（splitN 为 nil 时该标志无效果，即自动降级）。
func (s *Stream[T]) newParStage(n int) *Stream[T] {
	s.checkLinked()
	ud := s.drive
	return &Stream[T]{pipeline[T]{
		drive: func(down Sink[T], ec *evalCtx) { ud(down, ec) },
		chars: s.chars,
		parN:  n,
		splitN: func(k int) []any {
			if s.splitN == nil {
				return nil
			}
			return s.splitN(k)
		},
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

// evaluateParallel 执行并行分片求值：
// 分片 → 串行预创建各片终端（无竞争）→ 各片 goroutine 独立求值 →
// 按片序 total 合并 → 用户终端。
func evaluateParallel[T any](p *pipeline[T], down Sink[T], ec *evalCtx, pt parallelTotal[T]) {
	parts := p.splitN(p.parN)
	if len(parts) < 2 {
		p.drive(down, ec) // 不可分（或单份）：降级串行
		return
	}
	sinks := make([]Sink[T], len(parts))
	for i := range sinks {
		sinks[i] = pt.part() // 主 goroutine 串行创建：片级状态登记安全
	}
	var wg sync.WaitGroup
	wg.Add(len(parts))
	var panicMu sync.Mutex
	var panicVals []any
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
		}(i, parts[i])
	}
	wg.Wait()
	if len(panicVals) > 0 {
		panic(panicVals[0]) // 按捕获序 re-panic（近似首片 panic）
	}
	pt.total(sinks, down, ec) // 分片序合并
}

package stream

// Sink 是推送式消费者接口，是求值期间元素流动的通道。
//
// 求值引擎从终止操作出发，将各级中间操作的 Sink 逐级反向包装成一条链，
// 再由数据源单遍推动元素流过整条链（单遍融合）。
//
// 注意：接口方法不得声明自身类型参数（Go 1.27 语言限制），因此本接口
// 只使用 Stream 的元素类型 T。
type Sink[T any] interface {
	// Begin 在元素推送开始前调用一次，size 为源的估计元素数（未知为 -1），
	// 便于实现预分配容量等优化。
	Begin(size int64)
	// Accept 接收一个元素；返回 false 表示请求取消（短路），
	// 引擎将立即停止推动源并调用 End。
	Accept(t T) bool
	// End 在元素推送结束后调用一次（无论正常耗尽还是短路取消）。
	// 实现应在此完成收尾（如排序后统一输出）。
	End()
}

// sinkFunc 将"接受一个元素"的函数适配为最小 Sink，供内部使用。
// Begin/End 为空实现；未实现取消语义的算子可借助它快速装配。
type sinkFunc[T any] func(t T) bool

func (f sinkFunc[T]) Begin(int64)     {}
func (f sinkFunc[T]) Accept(t T) bool { return f(t) }
func (f sinkFunc[T]) End()            {}

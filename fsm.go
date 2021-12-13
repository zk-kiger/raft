package raft

// FSM 提供一个可以由客户端实现以使用日志复制的接口.
type FSM interface {

	// Apply 一旦提交了日志条目,就会调用应用日志.它返回一个值,如果该方法在
	// 与 FSM 相同的 Raft 节点上调用,则该值将在 Raft.Apply 方法返回的
	// ApplyFuture 中可用.
	Apply(*Log) interface{}
}

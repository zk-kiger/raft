package raft

import "errors"

var (
	// ErrorRaftShutdown 当操作请求不活跃的 Raft 时返回.
	ErrorRaftShutdown = errors.New("raft already is shutdown")
)

// Raft implements a Raft node.
type Raft struct {
	raftState

	// applyCh 异步地将 logs 发送给 leader,be committed and applied to FSM.
	//applyCh chan *logFuture

	// Raft 节点为 Leader 时使用.
	leaderState leaderState

	// 为 logs 提供持久化的存储.
	logs LogStore

	// 为 raftState 中的许多字段(持久化状态)提供了稳定的存储.
	stable StableStore

	// raft 节点间通信.
	trans Transport
}

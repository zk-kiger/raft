package raft

import "container/list"

// RaftState 记录了一个 Raft 节点的四种状态: Follower, Leader, Candidate, Shutdown.
type RaftState uint32

const (
	// Follower Raft 节点初始状态
	Follower RaftState = iota

	// Candidate Raft 节点有效状态之一
	Candidate

	// Leader Raft 节点有效状态之一
	Leader

	// Shutdown Raft 节点终止状态
	Shutdown
)

// raftState 包含 Raft 论文(Figure-2)中的状态变量
type raftState struct {
	// currentTerm, commitIndex, lastApplied 必须放在结构体顶部,这样可以保证它们是
	// 64 位对齐,这样做可以保证在 32 位平台下不会出现原子操作错误.

	// 当前任期(非易失性 - Cache for StableStore)
	currentTerm uint64

	// 已提交的最高日志条目索引
	commitIndex uint64

	// 应用到状态机的最高日志条目索引
	lastApplied uint64

	// 当前 Raft 节点状态
	state RaftState
}

// leaderState 该节点是 Leader 时,存储其他 Raft 节点的索引信息
type leaderState struct {
	commitCh   chan struct{}
	commitment *commitment
	inflight   *list.List // list of logFuture in log index order
	replState  map[ServerID]*followerReplication
}

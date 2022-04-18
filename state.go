package raft

import (
	"sync"
	"sync/atomic"
)

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

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// raftState 包含 Raft 论文(Figure-2)中的状态变量,并为变量提供线程安全的 set/get 方法.
type raftState struct {
	// currentTerm, commitIndex, lastApplied 必须放在结构体顶部,这样可以保证它们是
	// 64 位对齐,这样做可以保证在 32 位平台下不会出现原子操作错误.

	// 当前任期(非易失性 - Cache for StableStore)
	currentTerm uint64

	// 已提交的最高日志条目索引
	commitIndex uint64

	// 应用到状态机的最高日志条目索引
	lastApplied uint64

	// lastLock 保护下面四个字段.
	lastLock sync.Mutex

	// 缓存 latest snapshot index/term.
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64

	// 缓存 the latest log from LogStore.
	lastLogIndex uint64
	lastLogTerm  uint64

	// routinesGroup 用于处理多个 goroutine 并发竞争问题.
	routinesGroup sync.WaitGroup

	// 当前 Raft 节点状态
	state RaftState
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getLastLog() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastLog(index, term uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getLastSnapshot() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}

func (r *raftState) setLastSnapshot(index, term uint64) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

// getLastIndex 返回 stable store 的最后一个 index,可能来自 last log 或 last snapshot.
func (r *raftState) getLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return max(r.lastLogIndex, r.lastSnapshotIndex)
}

// getLastEntry 返回 stable store 的 last index 和 last term,可能来自 last log 或 last snapshot.
func (r *raftState) getLastEntry() (uint64, uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}

// goFunc 启动一个指定函数的 goroutine.
// 通过 sync.WaitGroup 来正确处理 goroutine 的开始,退出,增加,减少的竞争.
func (r *raftState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *raftState) waitShutdown() {
	r.routinesGroup.Wait()
}

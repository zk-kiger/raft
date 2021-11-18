package raft

import "sync"

type commitment struct {
	// matchIndexes, commitIndex 对论文中 leader state 记录的 nextIndex[], matchIndex[]
	// 的优化.论文设计中,nextIndex[] 记录投票者下一次复制日志条目索引,matchIndex[] 记录投票者
	// 准确的已提交日志条目索引.
	// matchIndexes 并非记录投票者准确的已提交日志条目索引,实际上记录了 commitIndex, AppendEntries
	// lastLogIndex 较大值.之所以这样做,考虑到 nextIndex[], matchIndex[] 都是非持久化数据,并且短暂
	// 的数据偏差(leader 分发 logs 时[还未 AppendEntries],matchIndexes 已经更新[match]到中位数索
	// 引)并不会造成任何问题.这样做降低了内存空间的使用且减少了 matchIndex 更新次数.

	// 保护 matchIndexes 和 commitIndex
	sync.Mutex

	// commitIndex 增长时进行通知,通知 leader 提交 inflight logs.
	commitCh chan struct{}

	// [投票者ID]:[已提交日志条目索引]
	// 优化:在 leader apply logs 时更新 matchIndexes,在 leader 后续接收到 AppendEntries 回复时,
	// 如果已提交日志条目索引小于 matchIndexes 记录的投票者ID的索引,就无需重复更新.
	matchIndexes map[ServerID]uint64

	// 记录 matchIndexes 的中位数索引,用于 leader 应用日志条目的索引
	commitIndex uint64

	// leader 任期中的第一个索引:这需要 leader 在提交任何日志条目之前复制到大多数集群节点
	// (Raft's commitment rule - Raft 论文section_5.4.3)
	startIndex uint64
}

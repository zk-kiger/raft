package raft

import (
	"sort"
	"sync"
)

// commitment 用于推进 leader's commit index. leader 和 replication goroutine 通过 match()
// 来报告新写入的日志条目,并在 commit index 被推进时,通知 commitCh.
type commitment struct {
	// matchIndexes, commitIndex 对论文中 leader state 记录的 nextIndex[], matchIndex[] 的优化.
	// 论文设计中,nextIndex[] 记录投票者下一次复制日志条目索引,matchIndex[] 记录投票者准确的已提交日志条目索引.
	// matchIndexes 并非记录投票者准确的已提交日志条目索引,实际上记录了 commitIndex, AppendEntries, lastLogIndex 较大值.

	// 之所以这样做,考虑到 nextIndex[], matchIndex[] 都是非持久化数据,并且短暂的数据偏差
	// (leader 分发 logs 时[还未 AppendEntries] commitIndex 已经更新[在match()方法中]到中位数索引)并不会造成任何问题.
	// 这样做降低了内存空间的使用且减少了 matchIndex 更新次数.

	// 保护 matchIndexes 和 commitIndex.
	sync.Mutex

	// commitIndex 增长时进行通知,通知 leader 提交 inflight logs.
	commitCh chan struct{}

	// [投票者ID]:[已提交日志条目索引]
	// 优化:在 leader apply logs 时更新 matchIndexes,在 leader 后续接收到 AppendEntries 回复时,
	// 如果已提交日志条目索引小于 matchIndexes 记录的投票者ID的索引,就无需重复更新.
	matchIndexes map[ServerID]uint64

	// 记录 matchIndexes 的中位数索引,用于 leader 应用日志条目的索引.
	commitIndex uint64

	// leader 任期中的第一个索引:这需要 leader 在提交任何日志条目之前复制到大多数集群节点
	// (Raft's commitment rule - Raft 论文section_5.4.3)
	startIndex uint64
}

// newCommitment 返回一个 commitment 结构体.
// 'configuration' 是集群中的服务器节点.
// 'startIndex' 是当前 term 第一个创建的索引.
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

// setConfiguration 在创建新的集群成员配置时调用,用于从现在开始确定 commitment.
// "configuration" 是集群中的服务器.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	// 重新计算 commitIndex.
	c.recalculate()
}

// getCommitIndex 在 commitCh 被通知之后被 leader 调用.
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// match 一旦服务器完成将日志条目写入磁盘,就会调用 match.
// leader 已写入新的日志条目或 follower 已回复 AppendEntries RPC.
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// recalculate 通过 matchIndexes 计算 commitIndex 的方法.
// 找到服务器已提交日志索引的中位数作为 commitIndex,符合多数派.
func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, commitIdx := range c.matchIndexes {
		matched = append(matched, commitIdx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]

	// 找到在我们任期内的 commitIndex,并通知 leader 尝试应用到客户端 fsm.
	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotifyCh(c.commitCh)
	}
}

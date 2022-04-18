package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound 给定的日志条目不可用.
	ErrLogNotFound = errors.New("log not found")
)

// followerReplication 负责在特定 term 内,从 leader 向远程 follower 发送快照和日志.
type followerReplication struct {
	// currentTerm, nextIndex 必须放在结构体顶部,这样可以保证它们是
	// 64 位对齐,这样做可以保证在 32 位平台下不会出现原子操作错误.

	// currentTerm 是该 leader 的 term,将包含在 AppendEntries 请求中.
	currentTerm uint64

	// nextIndex 是要发送给 follower 的下一个日志条目的索引,它可能会超过日志的末尾.
	nextIndex uint64

	// peer 包含远程的 follower 的网络地址和 ID.
	peer Server

	// commitment 跟踪 followers 确认的条目,以便 leader 的 commitIndex 可以推进.
	// 它会根据成功的 AppendEntries 响应进行更新.
	commitment *commitment

	// stopCh 当此 leader 下台或 follower 从集群中被删除时,会关闭/通知 stopCh.
	// 在 follower 删除的情况下,它带有一个日志索引;在退出之前,应尽最大努力尝试通过该索引进行复制.
	stopCh chan uint64

	// triggerCh 每次有新的日志条目需要追加给 follower 时,都会通知 triggerCh.
	triggerCh chan struct{}

	// triggerDeferErrorCh 用于提供 back channel.通过发送 deferErr,可以在复制完成时通知发送者.
	triggerDeferErrorCh chan *deferError

	// lastContact 每当收到来自 follower 的任何响应（成功与否）时, lastContact 都会更新为当前时间.
	// 用于检查 leader 是否应该下台(Raft.checkLeaderLease()).
	lastContact time.Time
	// lastContactLock 保护字段 'lastContact'.
	lastContactLock sync.RWMutex

	// failures 记录自从上次成功以来失败的 RPC 次数,用于 apply backoff(类似于熔断).
	failures uint64

	// stepDown 用于向 leader 通知下台,可能的原因是该 follower 的 term 大于 leader 的.
	stepDown chan struct{}

	// notifyCh 被通知发出心跳,用于检测这个服务器是否仍然是 leader.
	notifyCh chan struct{}
	// notify 是收到确认后要回应 map 中记录的 future,然后从 map 中移除.
	notify map[*verifyFuture]struct{}
	// notifyLock 保护 'notify'.
	notifyLock sync.Mutex
}

// notifyAll 用于通知所有等待的 verify future,如果服务器还相信我们是 leader.
func (s *followerReplication) notifyAll(leader bool) {
	// 消除等待通知.
	// 通过下面复制副本的方式可以最小化持锁时间.
	s.notifyLock.Lock()
	n := s.notify
	s.notify = make(map[*verifyFuture]struct{})
	s.notifyLock.Unlock()

	// 挨个 future 作出回应.
	for v := range n {
		v.vote(leader)
	}
}

// cleanNotify is used to delete notify, .
func (s *followerReplication) cleanNotify(v *verifyFuture) {
	s.notifyLock.Lock()
	delete(s.notify, v)
	s.notifyLock.Unlock()
}

// LastContact 返回最后一次联系的时间.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact 设置最后一次联系的时间为当前时间.
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

// replicate is a long running routine.
// 它将日志条目复制到单个 follower.
func (r *Raft) replicate(s *followerReplication) {
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:
			// 尽最大努力复制到该索引.
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return

		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}

		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)

		// 不是心跳机制,为了 follower 在 leader 提交自然流动中,快速了解 leader 的 commitIndex.
		// 真正的心跳机制不能做到这点.
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}
	}
	return
}

// replicateTo 是 replicate() 的助手,用于将日志复制到给定的最后一个索引.
// 如果追随者日志落后,我们会注意更新它们.
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	var req AppendEntriesRequest
	var resp AppendEntriesResponse

START:
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	// setup the request.
	if err := r.setupAppendEntries(s, &req, atomic.LoadUint64(&s.nextIndex), lastIndex); err == ErrLogNotFound {
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	// rpc call.
	if err := r.transport.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
		r.logger.Error("failed to appendEntries to", "peer", s.peer, "error", err)
		s.failures++
		return
	}

	// 新的 term,停止复制.
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	// 更新上一次联系时间.
	s.setLastContact()

	if resp.Success {
		// 更新复制状态,并清除错误次数.
		updateLastAppended(s, &req)
		s.failures = 0
	} else {
		// leader 最新日志项的前一个日志项和 follower 最新日志项任期或索引不匹配(follower 返回 resp.NoRetryBackoff),
		// leader 将日志复制的索引前移一个索引,在 CHECK_MORE 中判断,跳转 START 重新构建待复制的日志项.
		// 该流程会重复执行,直到日志项索引和任期匹配成功后,才会将 leader 日志强制复制给 follower
		atomic.StoreUint64(&s.nextIndex, max(min(s.nextIndex-1, resp.LastLog+1), 1))
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Warn("appendEntries rejected, sending older logs", "peer", s.peer, "next", atomic.LoadUint64(&s.nextIndex))
	}

CHECK_MORE:
	// 轮训 stopCh,防止我们正在循环中被要求停止.
	// 即使被要求复制到指定索引然后关闭的情况(maxIndex <- stopCh),最好不要循环发送大量日志条目给无论如何都会离开集群的落后者.
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// 检查,是否有更多的索引去复制.
	if atomic.LoadUint64(&s.nextIndex) <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP 用于获取一个日志失败时,通常是因为 follower 日志过于落后,我们必须发送快照作为替代.
SEND_SNAP:
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Error("failed to send snapshot to", "peer", s.peer, "error", err)
		return
	}

	// 检查,是否有更多的索引去复制.
	goto CHECK_MORE
}

// sendLatestSnapshot 向 follower 发送最新的快照.
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// get the snapshots.
	snapshots, err := r.snapshotStore.List()
	if err != nil {
		r.logger.Error("failed to list snapshots", "error", err)
		return false, err
	}

	// 至少有一个快照.
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// 打开最近的快照.
	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshotStore.Open(snapID)
	if err != nil {
		r.logger.Error("failed to open snapshot", "id", snapID, "error", err)
		return false, err
	}
	defer snapshot.Close()

	// setup the request.
	req := InstallSnapshotRequest{
		RPCHeader:          r.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               s.currentTerm,
		Leader:             r.transport.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Size:               meta.Size,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	// rpc call.
	var resp InstallSnapshotResponse
	if err = r.transport.InstallSnapshot(s.peer.ID, s.peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Error("failed to install snapshot", "id", snapID, "error", err)
		s.failures++
		return false, err
	}

	// 新的 term,停止复制.
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	// 更新上一次联系时间.
	s.setLastContact()

	if resp.Success {
		// update the indexes.
		atomic.StoreUint64(&s.nextIndex, meta.Index+1)
		s.commitment.match(s.peer.ID, meta.Index)

		s.failures = 0
		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Warn("installSnapshot rejected to", "peer", s.peer)
	}
	return false, nil
}

// heartbeat 用于定期调用节点上的 AppendEntries 以确保它们不会超时.
// 这是通过 replicate() 异步完成的,因为该 goroutine 可能会在磁盘IO上被阻塞.
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      s.currentTerm,
		Leader:    r.transport.EncodePeer(r.localID, r.localAddr),
	}
	var resp AppendEntriesResponse
	for {
		// 等待下一个心跳或者 leader 强制通知.
		select {
		case <-s.notifyCh:
		case <-randomTimeout(r.config().HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		if err := r.transport.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
			r.logger.Error("failed to heartbeat to", "peer", s.peer.Address, "error", err)
			r.observe(FailedHeartbeatObservation{PeerID: s.peer.ID, LastContact: s.LastContact()})
			failures++
			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			s.setLastContact()
			failures = 0
			s.notifyAll(resp.Success)
		}
	}
}

// setupAppendEntries 用于建立 AppendEntriesRequest.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	req.Leader = r.transport.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	return nil
}

// setPreviousLog 用于为 AppendEntriesRequest 设置 PrevLogEntry 和 PrevLogTerm.
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// 保护第一个索引,因为没有索引 0 处没有日志条目.
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogIndex = 0
		req.PrevLogTerm = 0

		// 防止 nextIndex 之前的索引是快照.
	} else if (nextIndex - 1) == lastSnapIdx {
		req.PrevLogIndex = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		if err := r.logStore.GetLog(nextIndex-1, &l); err != nil {
			r.logger.Error("failed to get log", "index", nextIndex-1, "error", err)
			return err
		}

		// set the previous index and term (0 if nextIndex is 1)
		req.PrevLogIndex = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs 用于建立 request 中需要被追加的日志条目.
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// 追加到 MaxAppendEntries 或 lastIndex.
	maxAppendEntries := r.config().MaxAppendEntries
	req.Entries = make([]*Log, 0, maxAppendEntries)
	index := min(nextIndex+uint64(maxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= index; i++ {
		oldLog := new(Log)
		if err := r.logStore.GetLog(i, oldLog); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// handleStaleTerm 当 follower 表示我们有一个过时的 term 时,调用该函数.
func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Error("peer has newer term, stopping replication", "peer", s.peer)
	// 不再是 leader,并通知 leader 下台,转移为 follower.
	s.notifyAll(false)
	asyncNotifyCh(s.stepDown)
}

// updateLastAppended 用于在接收到 AppendEntriesRequest 的响应后,更新 follower 复制状态信息.
func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {
	// 标记 inflight 日志为已提交.
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index+1)
		s.commitment.match(s.peer.ID, last.Index)
	}

	// 通知还是 leader.
	s.notifyAll(true)
}

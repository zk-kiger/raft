package raft

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"io/ioutil"
	"time"
)

const (
	minCheckInterval = 10 * time.Millisecond
)

var (
	KeyCurrentTerm       = []byte("CurrentTerm")
	KeyLastVoteTerm      = []byte("LastVoteTerm")
	KeyLastVoteCandidate = []byte("LastVoteCandidate")
)

// getRPCHeader returns an initialized RPCHeader struct for the given
// Raft instance. This structure is sent along with RPC requests and
// responses.
func (r *Raft) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: r.config().ProtocolVersion,
	}
}

// checkRPCHeader houses logic about whether this instance of Raft can process
// the given RPC message.
func (r *Raft) checkRPCHeader(rpc RPC) error {
	// Get the header off the RPC message.
	_, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC does not have a header")
	}
	return nil
}

// getSnapshotVersion returns the snapshot version that should be used when
// creating snapshots, given the protocol version in use.
func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {
	// Right now we only have two versions and they are backwards compatible
	// so we don't need to look at the protocol version.
	return 1
}

// commitTuple 用于发送给 fsm 待应用的已提交日志单元组.
// 'future' 关联 'log' 的 future,回复时被调用.
type commitTuple struct {
	log    *Log
	future *logFuture
}

// leaderState 该节点是 Leader 时,存储其他 Raft 节点的索引信息
type leaderState struct {
	commitCh   chan struct{}
	commitment *commitment
	inflight   *list.List // list of logFuture in log index order.
	replState  map[ServerID]*followerReplication
	notify     map[*verifyFuture]struct{}
	stepDown   chan struct{} // 用于通知 leader 下台.
}

// setLeader 用于修改当前集群中的 leader.
func (r *Raft) setLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	oldLeader := r.leader
	r.leader = leader
	r.leaderLock.Unlock()
	if oldLeader != leader {
		r.observe(LeaderObservation{Leader: leader})
	}
}

// requestConfigChange 发起配置更改请求函数.
// 'req' 配置更改请求参数.
func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	future := &configurationChangeFuture{
		req: req,
	}
	future.init()

	select {
	case <-timer:
		future.respond(ErrEnqueueTimeout)
		return future
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	}
}

// run 是一个长期运行 Raft FSM 的 goroutine.
func (r *Raft) run() {
	for {
		// 检查是否收到 shutdown.
		select {
		case <-r.shutdownCh:
			// 清除 leader,防止请求转发.
			r.setLeader("")
			return
		default:
		}

		// 根据节点状态进入 sub-FSM.
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the FSM for a follower.
func (r *Raft) runFollower() {
	var didWarn bool // warn once.
	r.logger.Info("entering follower state", "follower", r, "leader", r.Leader())
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout)

	for r.getState() == Follower {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case c := <-r.configurationChangeCh:
			// 不是 leader 时,拒绝一些操作.
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			// 不是 leader 时,拒绝一些操作.
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			// 不是 leader 时,拒绝一些操作.
			v.respond(ErrNotLeader)

		case u := <-r.userRestoreCh:
			// 不是 leader 时,拒绝一些操作.
			u.respond(ErrNotLeader)

		case <-heartbeatTimer:
			// restart the heartbeat timer.
			heartbeatTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(heartbeatTimeout)

			// 判断节点上次与 leader 联系距 now 的时间差是否心跳超时.
			contact := r.LastContact()
			if time.Now().Sub(contact) < heartbeatTimeout {
				continue
			}

			// 心跳失败,需要将状态转移到 candidate,并进行 election.
			lastLeader := r.Leader()
			r.setLeader("")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Warn("no known peers, aborting election")
					didWarn = true
				}
			} else if r.configurations.latestIndex == r.configurations.committedIndex &&
				!hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Warn("not part of stable configuration, aborting election")
					didWarn = true
				}
			} else {
				r.logger.Warn("heartbeat timeout reached, starting election", "last-leader", lastLeader)
				r.setState(Candidate)
				return
			}

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(r.liveBootstrap(b.configuration))

		case <-r.shutdownCh:
			return
		}
	}
}

// liveBootstrap 尝试为 cluster 分发初始配置.
func (r *Raft) liveBootstrap(configuration Configuration) error {
	cfg := r.config()
	if err := BootstrapCluster(&cfg, r.logStore, r.stableStore, r.snapshotStore, r.transport, configuration); err != nil {
		return err
	}

	// 使 configuration 生效.
	var entry Log
	if err := r.logStore.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	return r.processConfigurationLogEntry(&entry)
}

// runCandidate runs the FSM for a candidate.
func (r *Raft) runCandidate() {
	r.logger.Info("entering candidate state", "node", r, "term", r.getCurrentTerm()+1)

	// 开始投票,并且设置选举超时时间.
	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.config().ElectionTimeout)

	// 统计选票,并且计算法定人数(过半选举机制)
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	r.logger.Debug("votes needed: ", votesNeeded)

	for r.getState() == Candidate {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case vote := <-voteCh:
			// 如果存在比当前节点 term 还大的节点,当前节点不配作 leader.
			// 状态转移为 follower.
			if vote.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower")
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			// 检查是否投票,并增加投票个数.
			if vote.Granted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}

			// 获得一半以上的投票,当前节点成为 leader.
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won", "tally", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr)
				return
			}

		case c := <-r.configurationChangeCh:
			// 不是 leader 时,拒绝一些操作.
			c.respond(ErrNotLeader)

		case a := <-r.applyCh:
			// 不是 leader 时,拒绝一些操作.
			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:
			// 不是 leader 时,拒绝一些操作.
			v.respond(ErrNotLeader)

		case u := <-r.userRestoreCh:
			// 不是 leader 时,拒绝一些操作.
			u.respond(ErrNotLeader)

		case <-electionTimer:
			// 选举失败,重新选举!只是简单返回,将会退出 runCandidate.
			r.logger.Warn("Election timeout reached, restarting election")
			return

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case <-r.shutdownCh:
			return
		}
	}
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID
}

// electSelf 用于向集群配置中所有节点发送 RequestVote RPC,并为我们自己投票.
// 在方法中会增加 current term.返回的 channel 用于等待所有响应(包括对我们自己的投票).
// 只能从主线程调用.
func (r *Raft) electSelf() <-chan *voteResult {
	// 创建 voteResult channel.
	resultCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	// 增加 current term.
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	// 构建 RequestVote 结构体.
	lastIdx, lastTerm := r.getLastEntry()
	request := &RequestVoteRequest{
		RPCHeader:    r.getRPCHeader(),
		Term:         r.getCurrentTerm(),
		Candidate:    r.transport.EncodePeer(r.localID, r.localAddr),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// 构建函数用于请求节点投票.
	askPeer := func(peer Server) {
		r.goFunc(func() {
			resp := &voteResult{voterID: peer.ID}
			err := r.transport.RequestVote(peer.ID, peer.Address, request, &resp.RequestVoteResponse)
			if err != nil {
				r.logger.Error("failed to make requestVote RPC", "target", peer, "error", err)
				resp.Term = request.Term
				resp.Granted = false
			}
			resultCh <- resp
		})
	}

	// 向每个节点发送投票请求.
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				// 保留自己的投票.
				if err := r.persistVote(request.Term, request.Candidate); err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return nil
				}
				// resultCh 包括自己给自己的投票.
				resultCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      request.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				askPeer(server)
			}
		}
	}

	return resultCh
}

// persistVote 用于保留自己的投票,为了安全.
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stableStore.SetUint64(KeyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stableStore.Set(KeyLastVoteCandidate, candidate); err != nil {
		return err
	}
	return nil
}

// quorumSize 用于返回投票所需的法定人数,过半选举机制.
// 只能从主线程调用.
func (r *Raft) quorumSize() int {
	voters := 0
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

// runLeader runs the FSM for a leader.
// 此处进行设置,最终进入热循环 leaderLoop.
func (r *Raft) runLeader() {
	r.logger.Info("entering leader state", "leader", r)

	// Notify that we are the leader.
	overrideNotifyBool(r.leaderCh, true)

	// 设置 leader state.这个状态只能在 leaderLoop 中访问.
	r.setupLeaderState()

	// 清除 leader 期间的状态,当 leader 下台时.
	defer func() {
		// 由于我们之前是 leader,因此我们会在下台时更新我们的最后联系时间,
		// 这样我们就不会报告我们成为领导者之前的最后联系时间.
		// 否则,对于 client 来说,我们的数据似乎非常陈旧.
		r.setLastContact()

		// 停止复制(replication).
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		// 响应所有 inflight 日志.
		for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}

		// 清除所有 leader 状态.
		r.leaderState.commitCh = nil
		r.leaderState.commitment = nil
		r.leaderState.inflight = nil
		r.leaderState.replState = nil
		r.leaderState.stepDown = nil

		// 由于某些未知原因下台,没有已知的领导人.
		r.leaderLock.Lock()
		if r.leader == r.localAddr {
			r.leader = ""
		}
		r.leaderLock.Unlock()

		// Notify that we are not the leader.
		overrideNotifyBool(r.leaderCh, false)
	}()

	// 为每个节点启动 replication routine.
	r.startStopReplication()

	// 单节点变更只允许有一个未提交的配置条目,C_old 和 C_new 都不会破坏多数派,
	// 至少有一个节点相交.
	// 这里使用 no-op 日志有两个目的:
	//  1.保证选举限制(如果投票者自己的日志比候选人的日志更新，那么投票者就拒绝投票),
	//    但可能会出现旧的日志条目存储在大多数服务器上，但仍然可以被未来的领导者覆盖.
	//    为了解决这样的问题需要在新领导人接收客户端命令之前提交一个 no-op 携带自己任期号
	//    复制到大多数集群节点才能保证选举限制的成立.https://cii0nk6skx.feishu.cn/docs/doccn6315DzcJ5WWsyrfHtp7yRd#
	//  2.Raft 单步变更过程中如果发生 Leader 切换会出现正确性问题,可能导致已经提交的日志又被覆盖.
	//    https://zhuanlan.zhihu.com/p/359206808 这种情况应该是情况1的变体.
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.dispatchLogs([]*logFuture{noop})

	// 进入 leader loop 直到我们下台.
	r.leaderLoop()
}

func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1 /* 可能是当前 term 提交的第一个索引(noop log). */)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)
}

// setLastContact 设置最后一次联系时间为 now.
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

// startStopReplication 将设置状态并开始异步复制到新的节点,并停止复制到已删除的节点.
// 在删除节点之前,它会让 replication routine 尝试复制到最后的索引.
// 只能从主线程调用.
func (r *Raft) startStopReplication() {
	serverInConf := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIndex := r.getLastIndex()

	// 启动需要启动的 replication goroutine.
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}

		serverInConf[server.ID] = true

		repl, ok := r.leaderState.replState[server.ID]
		if !ok {
			r.logger.Info("added peer, starting replication", "peer", server.ID)
			repl = &followerReplication{
				peer:                server,
				commitment:          r.leaderState.commitment,
				stopCh:              make(chan uint64, 1),
				triggerCh:           make(chan struct{}, 1),
				triggerDeferErrorCh: make(chan *deferError, 1),
				currentTerm:         r.getCurrentTerm(),
				nextIndex:           lastIndex + 1,
				lastContact:         time.Now(),
				stepDown:            r.leaderState.stepDown,
				notify:              make(map[*verifyFuture]struct{}),
				notifyCh:            make(chan struct{}, 1),
			}

			r.leaderState.replState[server.ID] = repl
			r.goFunc(func() { r.replicate(repl) })
			// 异步地通知 channel,触发日志复制.
			asyncNotifyCh(repl.triggerCh)
			r.observe(PeerObservation{Peer: server, Removed: false})
		} else if ok && repl.peer.Address != server.Address {
			r.logger.Info("updating peer", "peer", server.ID)
			repl.peer = server
		}
	}

	// 停止需要停止的 replication goroutine.
	for serverID, repl := range r.leaderState.replState {
		if serverInConf[serverID] {
			continue
		}
		// 复制到 lastIndex 并停止.
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIndex)
		repl.stopCh <- lastIndex
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
		r.observe(PeerObservation{Peer: repl.peer, Removed: true})
	}
}

// dispatchLogs 被 leader 调用,将日志推送到磁盘,将日志标记为 inflight 并且开始复制它到其他节点.
func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()
	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()

	n := len(applyLogs)
	logs := make([]*Log, n)

	for idx, applyLog := range applyLogs {
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Term = term
		applyLog.log.Index = lastIndex
		applyLog.log.AppendedAt = now
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	// 将日志条目写入本地磁盘.
	if err := r.logStore.StoreLogs(logs); err != nil {
		r.logger.Error("failed to commit logs", "error", err)
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		r.setState(Follower)
		return
	}

	// 尝试更新 commitIndex,应用提交日志条目到客户端 fsm.
	r.leaderState.commitment.match(r.localID, lastIndex)

	// 更新 lastIndex,当写入磁盘成功时.
	r.setLastLog(lastIndex, term)

	// 通知 leader 将新的日志复制给其他节点,在 replicate() goroutine 接收执行.
	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}
}

// configurationChangeChIfSafe 如果可以安全地处理来自它的请求,
// 则返回 r.configurationChangeCh,否则返回 nil.
func (r *Raft) configurationChangeChIfSafe() chan *configurationChangeFuture {
	// 必须等待直到:
	// 	1.latest configuration 变为已提交,
	//  2.leader 在该 term 提交一些 entry(包括 noop).
	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

// leaderLoop 是 leader 的热循环.在完成 leader 的各种设置后调用.
func (r *Raft) leaderLoop() {
	// 用于追踪是否存在导致失去领导地位的日志存在.如果出现,我们不能并行处理日志.
	stepDown := false
	// 只用于第一次租约检查,后续会根据当前的配置值重新加载租约.
	lease := time.After(r.config().LeaderLeaseTimeout)

	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case <-r.leaderState.stepDown:
			r.setState(Follower)

		case newLog := <-r.applyCh:
			// TODO leadership transfer
			// 收集所有准备好的日志,一组进行提交.
			ready := []*logFuture{newLog}
		GroupCommitLoop:
			for i := 0; i < r.config().MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break GroupCommitLoop
				}
			}

			// 分发日志.
			if stepDown {
				// 正在卸任 leader,不处理任何新事务.
				for i := range ready {
					ready[i].respond(ErrNotLeader)
				}
			} else {
				r.dispatchLogs(ready)
			}

		case <-r.leaderState.commitCh:
			// 处理最新的提交日志条目.
			oldCommitIndex := r.getCommitIndex()
			commitIndex := r.leaderState.commitment.getCommitIndex()
			r.setCommitIndex(commitIndex)

			// 新的配置已提交,设置为已提交的值.
			if r.configurations.latestIndex > oldCommitIndex &&
				r.configurations.latestIndex <= commitIndex {
				r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
				// 我们已经不是投票者,需要下台.
				if !hasVote(r.configurations.committed, r.localID) {
					stepDown = true
				}
			}

			var groupReady []*list.Element
			var groupFutures = make(map[uint64]*logFuture)
			var lastIdxInGroup uint64

			// 将所有 inflight 的提交日志拉出队列.
			for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					// 不能超过 commitIndex.
					break
				}

				groupReady = append(groupReady, e)
				groupFutures[idx] = commitLog
				lastIdxInGroup = idx
			}

			// 处理 inflight 日志,在处理完之后,从 inflight 队列移除.
			if len(groupReady) != 0 {
				r.processLogs(lastIdxInGroup, groupFutures)

				for _, e := range groupReady {
					r.leaderState.inflight.Remove(e)
				}
			}

			if stepDown {
				if r.config().ShutdownOnRemove {
					r.logger.Info("removed ourself, shutting down")
					r.setState(Shutdown)
				} else {
					r.logger.Info("removed ourself, transitioning to follower")
					r.setState(Follower)
				}
			}

		case v := <-r.verifyCh:
			if v.quorumSize == 0 {
				// 分发给从节点,开始核实.
				r.verifyLeader(v)

			} else if v.votes < v.quorumSize {
				// 提前返回,言下之意存在一个新的 leader.
				r.logger.Warn("new leader elected, stepping down")
				r.setState(Follower)
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(ErrNotLeader)

			} else {
				// 法定人数同意,我们仍然是 leader.
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(nil)
			}

		case <-lease:
			// 检查我们是否超过租约,可能会下台.
			maxDiff := r.checkLeaderLease()

			// 下一个检查间隔应该针对我们联系的最后一个节点进行调整,而不是负数.
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			// 设置新的租约定时器.
			lease = time.After(checkInterval)

		case future := <-r.configurationsCh:
			// TODO leadership transfer
			future.configurations = r.configurations.Clone()
			future.respond(nil)

		case future := <-r.userRestoreCh:
			// TODO leadership transfer
			err := r.restoreUserSnapshot(future.meta, future.reader)
			future.respond(err)

		case future := <-r.configurationChangeChIfSafe():
			r.appendConfigurationEntry(future)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) restoreUserSnapshot(meta *SnapshotMeta, reader io.Reader) error {
	version := meta.Version
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return fmt.Errorf("unsupported snapshot version %d", version)
	}

	// 我们不支持在有配置变化时的快照,因为快照并没有办法表示这种状态.
	committedIndex := r.configurations.committedIndex
	latestIndex := r.configurations.latestIndex
	if committedIndex != latestIndex {
		return fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			latestIndex, committedIndex)
	}

	// 取消一些 inflight 的请求.
	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		e.Value.(*logFuture).respond(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}

	// 使用当前任期,索引较大的来覆盖快照元数据,重要的是,在索引中留一个空,
	// 这样我们就知道 Raft 日志没有什么东西,复制就会出现错误,并发送快照.
	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++

	sink, err := r.snapshotStore.Create(meta.Version, lastIndex, term,
		r.configurations.latest, r.configurations.latestIndex, r.transport)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	n, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if n != meta.Size {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", n, meta.Size)
	}
	if err = sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// 将快照恢复到 FSM.如果失败,会进入 bad state,直接 panic.
	future := &restoreFuture{
		ID: meta.ID,
	}
	future.init()
	select {
	case r.fsmMutateCh <- future:
		if err = future.Error(); err != nil {
			panic(fmt.Errorf("failed to restore snapshot: %v", err))
		}
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}

	// 我们设置了 last log,看起来我们已经存储了我们烧掉的空索引.last applied 的设置是因为我们让 FSM 获取快照状态,
	// 并且我们将 last snapshot 存储在稳定存储中,因为我们在此过程中创建了一个快照.
	r.setLastLog(lastIndex, term)
	r.setLastApplied(lastIndex)
	r.setLastSnapshot(lastIndex, term)

	r.logger.Info("restored user snapshot", "index", lastIndex)
	return nil
}

// verifyLeader 为安全起见,必须从主线程调用.使 Follower 尝试立即进行心跳.
func (r *Raft) verifyLeader(v *verifyFuture) {
	// 当前 leader,总是投票给自己.
	v.votes = 1

	v.quorumSize = r.quorumSize()
	if v.quorumSize == 1 {
		v.respond(nil)
		return
	}

	// Track this request.
	v.notifyCh = r.verifyCh
	r.leaderState.notify[v] = struct{}{}

	// 立即触发心跳.
	for _, repl := range r.leaderState.replState {
		repl.notifyLock.Lock()
		repl.notify[v] = struct{}{}
		repl.notifyLock.Unlock()
		asyncNotifyCh(repl.notifyCh)
	}
}

// appendConfigurationEntry 更改配置并将新的配置条目添加到日志中.
func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Info("updating configuration",
		"command", future.req.command,
		"server-id", future.req.serverID,
		"server-addr", future.req.serverAddress,
		"servers", hclog.Fmt("%+v", configuration.Servers))

	future.log = Log{
		Type: LogConfiguration,
		Data: EncodeConfiguration(configuration),
	}

	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.setLatestConfiguration(configuration, index)
	r.leaderState.commitment.setConfiguration(configuration)
	// 由于集群配置改变,需要重新启动相应的 replication goroutine.
	r.startStopReplication()
}

// processLogs 用于应用还未到给定索引的提交日志到 fsm.
// 该函数可以被 leader 或 follower 调用.
// Followers 从 AppendEntries 调用,一次 n 个日志条目, 'futures' 传递 nil.
// Leaders 在日志条目被提交时调用, 'futures' 传递 inflight logs.
func (r *Raft) processLogs(index uint64, futures map[uint64]*logFuture) {
	// 拒绝我们已经应用过的日志.
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Warn("skipping already applied logs", "index", index)
		return
	}

	applyBatch := func(batch []*commitTuple) {
		select {
		case r.fsmMutateCh <- batch:
		case <-r.shutdownCh:
			for _, cl := range batch {
				if cl.future != nil {
					cl.future.respond(ErrRaftShutdown)
				}
			}
		}
	}

	// 防止 MaxAppendEntries 变得可重新加载.
	maxAppendEntries := r.config().MaxAppendEntries
	batch := make([]*commitTuple, 0, maxAppendEntries)

	// 应用先前的日志.
	for i := lastApplied + 1; i <= index; i++ {
		var preparedLog *commitTuple
		// 从 future 或 logStore 获取 log.
		future, futureOk := futures[i]
		if futureOk {
			preparedLog = r.prepareLog(&future.log, future)
		} else {
			l := &Log{}
			if err := r.logStore.GetLog(i, l); err != nil {
				r.logger.Error("failed to get log", "index", i, "error", err)
				panic(err)
			}
			preparedLog = r.prepareLog(l, nil)
		}

		switch {
		case preparedLog != nil:
			// 如果存在准备好的发送给 fsm 的日志,添加到 batch.
			// fsm 线程会响应 future.
			batch = append(batch, preparedLog)

			// 如果 batch 达到阈值,先发送给 fsm.
			if len(batch) >= maxAppendEntries {
				applyBatch(batch)
				batch = make([]*commitTuple, 0, maxAppendEntries)
			}

		case futureOk:
			// 如果给了 future,就调用.
			future.respond(nil)
		}
	}

	// 将 batch 剩余的 logs,发送给 fsm.
	if len(batch) != 0 {
		applyBatch(batch)
	}

	r.setLastApplied(index)
}

// prepareLog 用来处理单个日志条目,例如 LogNoop 会被忽略不需要提交到 FSM.
func (r *Raft) prepareLog(l *Log, future *logFuture) *commitTuple {
	switch l.Type {
	case LogBarrier:
		// Barrier is handled by the FSM.
		fallthrough

	case LogCommand:
		return &commitTuple{l, future}

	case LogConfiguration:
		// Only support this with the v2 configuration format.
		if r.protocolVersion > 2 {
			return &commitTuple{l, future}
		}
	case LogNoop:
		// Ignore the no-op.

	default:
		panic(fmt.Errorf("unrecognized log type: %#v", l))
	}

	return nil
}

// checkLeaderLease 用于检查我们是否可以在最后一个领导租约间隔内联系法定人数的节点.
// 如果没有,我们需要下台,因为我们可能已经失去了连接.返回无接触的最长持续时间.
// 只能从主线程调用.
func (r *Raft) checkLeaderLease() time.Duration {
	// 记录可以联系到的节点个数,自己一直可以联系到自己.
	contacted := 0

	// 租约超时,用于后续循环.
	leaseTimeout := r.config().LeaderLeaseTimeout

	// 检查每个 follower.
	var maxDiff time.Duration
	now := time.Now()
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				contacted++
				continue
			}
			f := r.leaderState.replState[server.ID]
			diff := now.Sub(f.LastContact())
			if diff <= leaseTimeout {
				contacted++
				if diff > maxDiff {
					maxDiff = diff
				}
			} else {
				if diff <= 3*leaseTimeout {
					r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)
				} else {
					r.logger.Debug("failed to contact", "server-id", server.ID, "time", diff)
				}
			}
		}
	}

	// 法定人数.
	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Warn("failed to contact quorum of nodes, stepping down")
		r.setState(Follower)
	}
	return maxDiff
}

// processRPC 被调用处理来自 transport layer 接收的到 RPC request.
// 只能从主线程调用.
func (r *Raft) processRPC(rpc RPC) {
	if err := r.checkRPCHeader(rpc); err != nil {
		rpc.Respond(nil, err)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)

	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)

	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)

	default:
		r.logger.Error("got unexpected command", "command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// processHeartbeat 是一个特殊的处理程序,仅用于心跳请求,以便在传输支持的情况下可以快速处理它们.
// 只能从主线程调用.
func (r *Raft) processHeartbeat(rpc RPC) {
	// 如果 shutdown,就忽略 RPC.
	select {
	case <-r.shutdownCh:
		return
	default:
	}

	// 确保该方法只处理心跳请求.
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Error("expected heartbeat, but got command: ", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// appendEntries is invoked when we get an append entries RPC call.
// 只能从主线程调用.
func (r *Raft) appendEntries(rpc RPC, req *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		LastLog:   r.getLastIndex(),
		Success:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 忽略 older term.
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring appendEntries request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// 如果我们收到了更新的 term,如果我们收到了 appendEntries 调用,转换为 follower.
	if req.Term > r.getCurrentTerm() || r.getState() != Follower {
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// 保存当前 term 的 leader.
	r.setLeader(r.transport.DecodePeer(req.Leader))

	// 验证 last log entry.
	// 对于日志项差异的情况,无需重试(NoRetryBackoff = true).
	if req.PrevLogIndex > 0 {
		lastIndex, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if req.PrevLogIndex == lastIndex {
			prevLogTerm = lastTerm
		} else {
			var preLog Log
			if err := r.logStore.GetLog(req.PrevLogIndex, &preLog); err != nil {
				r.logger.Warn("failed to get previous log",
					"previous-index", req.PrevLogIndex,
					"last-index", lastIndex,
					"error", err)
				resp.NoRetryBackoff = true
				return
			}
		}

		if req.PrevLogTerm != prevLogTerm {
			r.logger.Warn("previous log term mis-match",
				"ours", prevLogTerm,
				"remote", req.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	// 处理新的追加日志条目.
	if len(req.Entries) > 0 {
		// 删除冲突日志,跳过一些日志副本.
		lastLogIndex, _ := r.getLastLog()
		var appendEntries []*Log
		for i, entry := range req.Entries {
			// 如果 entry.index > lastLogIndex,后续的日志条目直接追加.
			if entry.Index > lastLogIndex {
				appendEntries = req.Entries[i:]
				break
			}

			var storeLogEntry Log
			if err := r.logStore.GetLog(entry.Index, &storeLogEntry); err != nil {
				r.logger.Warn("failed to get log entry",
					"index", entry.Index,
					"error", err)
				rpcErr = err
				return
			}
			// 日志项存在冲突.
			// 解决冲突:删除冲突日志项到 lastLogIndex 之间的日志.
			if entry.Term != storeLogEntry.Term {
				r.logger.Warn("clearing log suffix",
					"from", entry.Index,
					"to", lastLogIndex)
				if err := r.logStore.DeleteRange(entry.Index, lastLogIndex); err != nil {
					r.logger.Error("failed to clear log suffix", "error", err)
					rpcErr = err
					return
				}
				// 删除了部分日志条目, LatestConfiguration 需要回退到已提交配置.
				if entry.Index <= r.configurations.latestIndex {
					r.setLatestConfiguration(r.configurations.committed, r.configurations.committedIndex)
				}
				appendEntries = req.Entries[i:]
				break
			}
		}

		if n := len(appendEntries); n > 0 {
			// 追加新的日志条目.
			if err := r.logStore.StoreLogs(appendEntries); err != nil {
				r.logger.Error("failed to append to logs", "error", err)
				rpcErr = err
				return
			}

			// 处理新配置改变.
			for _, entry := range appendEntries {
				if err := r.processConfigurationLogEntry(entry); err != nil {
					r.logger.Warn("failed to append entry",
						"index", entry.Index,
						"error", err)
					rpcErr = err
					return
				}
			}

			// 更新 last log index.
			last := appendEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}
	}

	// 更新 commitIndex.
	// 根据领导者最新提交的日志项索引,来计算当前需要被应用的日志项,并应用到 fsm.
	if req.LeaderCommitIndex > 0 && req.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(req.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		}
		r.processLogs(idx, nil)
	}

	// over! set success.
	resp.Success = true
	r.setLastContact()
	return
}

// requestVote is invoked when we get an request vote RPC call.
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	r.observe(*req)

	resp := &RequestVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 如果有已知的 leader,拒绝投票.
	candidate := r.transport.DecodePeer(req.Candidate)
	if leader := r.Leader(); leader != "" && leader != candidate {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", candidate,
			"leader", leader)
		return
	}

	// 忽略 older term.
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring vote request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// 如果遇到更新的 term,增加 term.
	if req.Term > r.getCurrentTerm() {
		// 确保转移为 follower.
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	lastVoteTerm, err := r.stableStore.GetUint64(KeyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote term", "error", err)
		rpcErr = err
		return
	}
	lastVoteCandidate, err := r.stableStore.Get(KeyLastVoteCandidate)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote candidate", "error", err)
		rpcErr = err
		return
	}
	// 检查这轮 term 是否已经投票.
	if req.Term == lastVoteTerm && lastVoteCandidate != nil {
		r.logger.Info("duplicate requestVote for same term", "term", req.Term)
		if bytes.Compare(lastVoteCandidate, req.Candidate) == 0 {
			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true
		}
		return
	}

	lastIndex, lastTerm := r.getLastEntry()
	// 如果我们最后日志的 term 更新,就拒绝.
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}
	// 如果我们最后日志的索引更大,就拒绝.
	if lastTerm == req.LastLogTerm && lastIndex > req.LastLogIndex {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIndex,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	// 为了安全,保存投票信息.
	if err = r.persistVote(req.Term, req.Candidate); err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		rpcErr = err
		return
	}

	resp.Granted = true
	r.setLastContact()
	return
}

// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// 为此,我们必须处于 follower 状态,因为这意味着我们在日志重放方面落后于 leader 太远了.
// 只能从主线程调用.
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		io.Copy(ioutil.Discard, rpc.Reader) // 确保我们始终使用流中的所有数据.
		rpc.Respond(resp, rpcErr)
	}()

	// 忽略 older term.
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring installSnapshot request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// 如果遇到更新的 term,增加 term.
	if req.Term > r.getCurrentTerm() {
		// 确保转移为 follower.
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// 保存当前 term 的 leader.
	r.setLeader(r.transport.DecodePeer(req.Leader))

	// 创建快照.
	reqConfiguration := DecodeConfiguration(req.Configuration)
	reqConfigurationIndex := req.ConfigurationIndex
	sink, err := r.snapshotStore.Create(req.SnapshotVersion, req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex, r.transport)
	if err != nil {
		r.logger.Error("failed to create snapshot to install", "error", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	// 将远程快照写入磁盘.
	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		sink.Cancel()
		r.logger.Error("failed to copy snapshot", "error", err)
		rpcErr = err
		return
	}

	// 检查是否接收完.
	if n != req.Size {
		sink.Cancel()
		r.logger.Error("failed to receive whole snapshot",
			"received", hclog.Fmt("%d / %d", n, req.Size))
		rpcErr = fmt.Errorf("short read")
		return
	}

	// 完成快照.
	if err = sink.Close(); err != nil {
		r.logger.Error("failed to finalize snapshot", "error", err)
		rpcErr = err
		return
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// restore snapshot to fsm.
	future := &restoreFuture{ID: sink.ID()}
	future.ShutdownCh = r.shutdownCh
	future.init()
	select {
	case r.fsmMutateCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// wait for the restore to happen.
	if err = future.Error(); err != nil {
		r.logger.Error("failed to restore snapshot", "error", err)
		rpcErr = err
		return
	}

	// 更新 lastApplied,因为我们不能重放日志.
	r.setLastApplied(req.LastLogIndex)

	// 更新快照信息.
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	r.setLatestConfiguration(reqConfiguration, reqConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, reqConfigurationIndex)

	// 压缩日志,即使失败也要继续.
	if err = r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Error("failed to compact logs", "error", err)
	}

	r.logger.Info("Installed remote snapshot")
	resp.Success = true
	r.setLastContact()
	return
}

// processConfigurationLogEntry 获取一个日志条目并在该条目产生新配置时更新最新配置及已提交配置.
// 只能从主线程调用,或者在任何线程开始之前从 NewRaft() 调用.
func (r *Raft) processConfigurationLogEntry(entry *Log) error {
	switch entry.Type {
	case LogConfiguration:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)
	}
	return nil
}

// setCurrentTerm 以持久化的方式设置 current term.
func (r *Raft) setCurrentTerm(t uint64) {
	// 先持久化到磁盘.
	if err := r.stableStore.SetUint64(KeyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err))
	}
	r.raftState.setCurrentTerm(t)
}

// setState 用于更新当前状态.任何状态转换都会导致已知领导者被清除.
// 意味着只有在更新状态后才应该设置领导者.
func (r *Raft) setState(state RaftState) {
	r.setLeader("")
	oldState := r.raftState.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}

// setLatestConfiguration 存储最新的配置并更新之前存储配置的副本.
func (r *Raft) setLatestConfiguration(c Configuration, i uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = i
	r.latestConfiguration.Store(c.Clone())
}

// setCommittedConfiguration 存储已提交的配置.
func (r *Raft) setCommittedConfiguration(c Configuration, i uint64) {
	r.configurations.committed = c
	r.configurations.committedIndex = i
}

// getLatestConfiguration 从主配置的副本中读取配置,这意味着它可以独立于主循环进行访问.
func (r *Raft) getLastConfiguration() Configuration {
	// 用于处理预先未设置配置的情况下被调用的处理方式.
	switch c := r.latestConfiguration.Load().(type) {
	case Configuration:
		return c
	default:
		return Configuration{}
	}
}

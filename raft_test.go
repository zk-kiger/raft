package raft

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRaft_StartStop(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
}

func TestRaft_AfterShutdown(t *testing.T) {
	c := MakeCluster(1, t, nil)
	c.Close()
	raft := c.rafts[0]

	// Everything should fail now
	if f := raft.Apply(nil, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}

	if f := raft.AddVoter(ServerID("id"), ServerAddress("addr"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.AddNonvoter(ServerID("id"), ServerAddress("addr"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.RemoveServer(ServerID("id"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.DemoteVoter(ServerID("id"), 0, 0); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}
	if f := raft.Snapshot(); f.Error() != ErrRaftShutdown {
		t.Fatalf("should be shutdown: %v", f.Error())
	}

	// 保证幂等.
	if f := raft.Shutdown(); f.Error() != nil {
		t.Fatalf("shutdown should be idempotent")
	}
}

func TestRaft_LiveBootstrap(t *testing.T) {
	c := MakeClusterNoBootstrap(3, t, nil)
	defer c.Close()

	configuration := Configuration{}
	for _, r := range c.rafts {
		server := Server{
			ID:      r.localID,
			Address: r.localAddr,
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	boot := c.rafts[0].BootstrapCluster(configuration)
	if err := boot.Error(); err != nil {
		t.Fatalf("bootstrap err: %v", err)
	}

	// 应该有一位 leader.
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// 应该被应用.
	future := leader.Apply([]byte("test"), c.conf.CommitTimeout)
	if err := future.Error(); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	c.WaitForReplication(1)

	// 确保启动后,bootstrap 程序失败.
	boot = c.rafts[0].BootstrapCluster(configuration)
	if err := boot.Error(); err != ErrCantBootstrap {
		t.Fatalf("bootstrap should have failed: %v", err)
	}
}

func TestRaft_RecoverCluster_NoState(t *testing.T) {
	c := MakeClusterNoBootstrap(1, t, nil)
	defer c.Close()

	r := c.rafts[0]
	configuration := Configuration{
		Servers: []Server{
			{
				ID:      r.localID,
				Address: r.localAddr,
			},
		},
	}
	cfg := r.config()
	err := RecoverCluster(&cfg, &MockFSM{}, r.logStore, r.stableStore,
		r.snapshotStore, r.transport, configuration)
	if err == nil || !strings.Contains(err.Error(), "no initial state") {
		t.Fatalf("should have failed for no initial state: %v", err)
	}
}

func TestRaft_RecoverCluster(t *testing.T) {
	snapshotThreshold := 5
	runRecover := func(t *testing.T, applies int) {
		var err error
		conf := inmemConfig(t)
		conf.TrailingLogs = 10
		conf.SnapshotThreshold = uint64(snapshotThreshold)
		c := MakeCluster(3, t, conf)
		defer c.Close()

		// 提交一些日志.
		c.logger.Debug("running with", "applies", applies)
		leader := c.Leader()
		for i := 0; i < applies; i++ {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
			if err = future.Error(); err != nil {
				t.Fatalf("apply err: %v", err)
			}
		}

		// 获取当前配置的快照.
		future := leader.GetConfiguration()
		if err = future.Error(); err != nil {
			t.Fatalf("get configuration err: %v", err)
		}
		configuration := future.Configuration()

		// 关闭所有 raft 节点.
		for _, raft := range c.rafts {
			if err = raft.Shutdown().Error(); err != nil {
				t.Fatalf("shutdown err: %v", err)
			}
		}

		// 恢复集群.我们需要重置 FSM、Transport,没有状态可以被保留.
		for i, r := range c.rafts {
			var before []*SnapshotMeta
			before, err = r.snapshotStore.List()
			if err != nil {
				t.Fatalf("snapshot list err: %v", err)
			}
			cfg := r.config()
			if err = RecoverCluster(&cfg, r.fsm, r.logStore, r.stableStore, r.snapshotStore,
				r.transport, configuration); err != nil {
				t.Fatalf("recover cluster err: %v", err)
			}

			var after []*SnapshotMeta
			after, err = r.snapshotStore.List()
			if err != nil {
				t.Fatalf("snapshot list err: %v", err)
			}
			if len(after) != len(before)+1 {
				t.Fatalf("expected a new snapshot, %d vs. %d", len(before), len(after))
			}
			var first uint64
			first, err = r.logStore.FirstIndex()
			if err != nil {
				t.Fatalf("first log index err: %v", err)
			}
			var last uint64
			last, err = r.logStore.LastIndex()
			if err != nil {
				t.Fatalf("last log index err: %v", err)
			}
			if first != 0 || last != 0 {
				t.Fatalf("expected empty logs, got %d/%d", first, last)
			}

			// 启动手动恢复的 raft 实例.
			_, transport := NewInmemTransport(r.localAddr)
			var r2 *Raft
			r2, err = NewRaft(&cfg, &MockFSM{}, r.logStore, r.stableStore, r.snapshotStore, transport)
			if err != nil {
				t.Fatalf("new raft err: %v", err)
			}
			c.rafts[i] = r2
			c.fsms[i] = r2.fsm.(*MockFSM)
			c.trans[i] = r2.transport.(*InmemTransport)
		}
		c.FullyConnect()
		time.Sleep(c.propagateTimeout * 3)

		// 让事情解决完,并确保恢复.
		c.EnsureLeader(t, c.Leader().localAddr)
		c.EnsureSame(t)
		c.EnsureSamePeers(t)
	}

	t.Run("no snapshot, no trailing logs", func(t *testing.T) {
		runRecover(t, 0)
	})
	t.Run("no snapshot, some trailing logs", func(t *testing.T) {
		runRecover(t, snapshotThreshold-1)
	})
	t.Run("snapshot, with trailing logs", func(t *testing.T) {
		runRecover(t, snapshotThreshold+10)
	})
}

func TestRaft_HasExistingState(t *testing.T) {
	var err error
	// Make a cluster.
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// Make a new cluster of 1.
	c1 := MakeClusterNoBootstrap(1, t, nil)

	// Make sure the initial state is clean.
	var hasState bool
	hasState, err = HasExistingState(c1.rafts[0].logStore, c1.rafts[0].stableStore, c1.rafts[0].snapshotStore)
	if err != nil || hasState {
		t.Fatalf("should not have any existing state, %v", err)
	}

	// Merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// Join the new node in.
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err = future.Error(); err != nil {
		t.Fatalf("[ERR] err: %v", err)
	}

	// Check the FSMs.
	c.EnsureSame(t)

	// Check the peers.
	c.EnsureSamePeers(t)

	// Ensure one leader.
	c.EnsureLeader(t, c.Leader().localAddr)

	// Make sure it's not clean.
	hasState, err = HasExistingState(c1.rafts[0].logStore, c1.rafts[0].stableStore, c1.rafts[0].snapshotStore)
	if err != nil || !hasState {
		t.Fatalf("should have some existing state, %v", err)
	}
}

func TestRaft_SingleNode(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(1, t, conf)
	defer c.Close()
	r := c.rafts[0]

	// watch leaderCh for change.
	select {
	case v := <-r.LeaderCh():
		if !v {
			t.Fatalf("should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		t.Fatalf("timeout becoming leader")
	}

	// should become leader.
	if s := r.State(); s != Leader {
		t.Fatalf("expected leader: %s", s)
	}

	// should be able to apply.
	future := r.Apply([]byte("test"), 0)
	if err := future.Error(); err != nil {
		t.Fatalf("apply err: %v", err)
	}

	// check response, If successful, Response() will return the number of committed logs.
	if future.Response().(int) != 1 {
		t.Fatalf("bad response: %v", future.Response())
	}

	// check index.
	if idx := future.Index(); idx == 0 {
		t.Fatalf("bad index: %d", idx)
	}

	// check that it is applied to FSM.
	if len(getMockFSM(c.fsms[0]).logs) != 1 {
		t.Fatalf("did not apply to FSM!")
	}
}

func TestRaft_TripleNode(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// should be one leader.
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// should be able to apply.
	if err := leader.Apply([]byte("test"), 0).Error(); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	c.WaitForReplication(1)
}

func TestRaft_LeaderFail(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// should be one leader.
	c.Followers()
	leader := c.Leader()

	// should be able to apply.
	if err := leader.Apply([]byte("test"), 0).Error(); err != nil {
		t.Fatalf("apply err: %v", err)
	}
	c.WaitForReplication(1)

	// disconnect the leader now.
	t.Logf("[INFO] Disconnect %v", leader)
	leaderTerm := leader.getCurrentTerm()
	c.Disconnect(leader.localAddr)

	// wait for new leader.
	limit := time.Now().Add(c.longStopTimeout)
	var newLeader *Raft
	for time.Now().Before(limit) && newLeader == nil {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders := c.GetInState(Leader)
		if len(leaders) == 1 && leaders[0] != leader {
			newLeader = leaders[0]
		}
	}
	if newLeader == nil {
		t.Fatalf("expected new leader")
	}

	// ensure newLeader term greater.
	if newLeader.getCurrentTerm() <= leaderTerm {
		t.Fatalf("expected newer term! %d %d (%v, %v)", newLeader.getCurrentTerm(), leaderTerm, newLeader, leader)
	}

	// apply should work not on old leader.
	future1 := leader.Apply([]byte("fail"), c.conf.CommitTimeout)

	// apply should work on new leader.
	future2 := newLeader.Apply([]byte("apply"), c.conf.CommitTimeout)

	if err := future2.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// recover network.
	t.Logf("[INFO] Reconnecting %v", leader)
	c.FullyConnect()

	if err := future1.Error(); err != ErrLeadershipLost && err != ErrNotLeader {
		t.Fatalf("err: %v", err)
	}

	// wait for log replication.
	c.EnsureSame(t)

	// check two entries are applied to the FSM.
	for _, fsmRaw := range c.fsms {
		fsm := getMockFSM(fsmRaw)
		fsm.Lock()
		if len(fsm.logs) != 2 {
			t.Fatalf("did not apply both to FSM! %v", fsm.logs)
		}
		if bytes.Compare(fsm.logs[0], []byte("test")) != 0 {
			t.Fatalf("first entry should be 'test'")
		}
		if bytes.Compare(fsm.logs[1], []byte("apply")) != 0 {
			t.Fatalf("second entry should be 'apply'")
		}
		fsm.Unlock()
	}
}

func TestRaft_BehindFollower(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// disconnect one follower.
	leader := c.Leader()
	followers := c.Followers()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// commit a lot of logs.
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// wait for the last future to apply.
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// check that we have a non zero last contact.
	if behind.LastContact().IsZero() {
		t.Fatalf("expected previous contact")
	}

	// reconnect the behind node.
	c.FullyConnect()

	// ensure all the logs are the same.
	c.EnsureSame(t)

	// ensure one leader.
	leader = c.Leader()
	c.EnsureLeader(t, leader.localAddr)
}

func TestRaft_ApplyNonLeader(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// wait for leader.
	c.Leader()

	followers := c.GetInState(Follower)
	if len(followers) != 2 {
		t.Fatalf("expected 2 follower")
	}
	follower := followers[0]

	// try to apply.
	future := follower.Apply([]byte("test"), c.conf.CommitTimeout)
	if future.Error() != ErrNotLeader {
		t.Fatalf("should not apply on follower")
	}

	// the error should be cached.
	if future.Error() != ErrNotLeader {
		t.Fatalf("should not apply on follower")
	}
}

func TestRaft_ApplyConcurrent(t *testing.T) {
	conf := inmemConfig(t)
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// wait for leader.
	leader := c.Leader()

	const GR = 100
	var group sync.WaitGroup
	group.Add(GR)

	applyFunc := func(i int) {
		defer group.Done()
		future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
		if err := future.Error(); err != nil {
			c.Failf("[ERR] err: %v", err)
		}
	}

	// concurrently apply.
	for i := 0; i < GR; i++ {
		go applyFunc(i)
	}

	// wait for finish.
	doneCh := make(chan struct{})
	go func() {
		group.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(c.longStopTimeout):
		t.Fatalf("timeout")
	}

	// 如果有事情已经失败,那么就立即停止.
	if t.Failed() {
		t.Fatalf("One or more of the apply operations failed")
	}

	// check the fsm.
	c.EnsureSame(t)
}

func TestRaft_ApplyConcurrent_Timeout(t *testing.T) {
	conf := inmemConfig(t)
	conf.CommitTimeout = 1 * time.Millisecond
	conf.HeartbeatTimeout = 2 * conf.HeartbeatTimeout
	conf.ElectionTimeout = 2 * conf.ElectionTimeout
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// wait for a leader.
	leader := c.Leader()

	// enough enqueues should cause at least one timeout...
	var didTimeout int32
	for i := 0; (i < 5000) && (atomic.LoadInt32(&didTimeout) == 0); i++ {
		go func(i int) {
			future := leader.Apply([]byte(fmt.Sprintf("test%d", i)), time.Microsecond)
			if future.Error() == ErrEnqueueTimeout {
				atomic.StoreInt32(&didTimeout, 1)
			}
		}(i)

		// give the leader loop some other things to do in order to
		// increase the odds of a timeout.
		if i%5 == 0 {
			leader.VerifyLeader()
		}
	}

	// loop until we see a timeout, or give up.
	limit := time.Now().Add(c.longStopTimeout)
	for time.Now().Before(limit) {
		if atomic.LoadInt32(&didTimeout) != 0 {
			return
		}
		c.WaitEvent(nil, c.propagateTimeout)
	}
	t.Fatalf("Timeout waiting to detect apply timeouts")
}

func TestRaft_JoinNode(t *testing.T) {
	c := MakeCluster(2, t, nil)
	defer c.Close()

	// make a new cluster of 1.
	c1 := MakeClusterNoBootstrap(1, t, nil)

	// merge clusters.
	c.Merge(c1)
	c.FullyConnect()

	// join the node in.
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("add voter err: %v", err)
	}

	// ensure one leader.
	c.EnsureLeader(t, c.Leader().localAddr)

	// check fsm.
	c.EnsureSame(t)

	// check peers.
	c.EnsureSamePeers(t)
}

func TestRaft_JoinNode_ConfigStore(t *testing.T) {
	conf := inmemConfig(t)
	c := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      true,
		Conf:           conf,
		ConfigStoreFSM: true,
	})
	defer c.Close()

	// make a new cluster node.
	c1 := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      false,
		Conf:           conf,
		ConfigStoreFSM: true,
	})
	c2 := makeCluster(t, &MakeClusterOpts{
		Peers:          1,
		Bootstrap:      false,
		Conf:           conf,
		ConfigStoreFSM: true,
	})

	// merge clusters.
	c.Merge(c1)
	c.Merge(c2)
	c.FullyConnect()

	// join the new node in.
	future := c.Leader().AddVoter(c1.rafts[0].localID, c1.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	// join the new node in.
	future = c.Leader().AddVoter(c2.rafts[0].localID, c2.rafts[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// ensure one leader.
	c.EnsureLeader(t, c.Leader().localAddr)

	// check fsm.
	c.EnsureSame(t)

	// check peers.
	c.EnsureSamePeers(t)

	// check the fsm hold the correct config log.
	for _, fsmRaw := range c.fsms {
		fsm := getMockFSM(fsmRaw)
		if len(fsm.configurations) != 3 {
			t.Fatalf("unexpected number of configuration changes: %d", len(fsm.configurations))
		}
		if len(fsm.configurations[0].Servers) != 1 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[0].Servers)
		}
		if len(fsm.configurations[1].Servers) != 2 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[1].Servers)
		}
		if len(fsm.configurations[2].Servers) != 3 {
			t.Fatalf("unexpected number of servers in config change: %v", fsm.configurations[2].Servers)
		}
	}

}

func TestRaft_RemoveFollower(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()

	// wait for has 2 follower.
	var followers []*Raft
	limit := time.Now().Add(c.longStopTimeout)
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.propagateTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected tow follower: %d", len(followers))
	}

	// remove a follower.
	follower := followers[0]
	future := leader.RemoveServer(follower.localID, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// wait a while.
	c.WaitEvent(nil, c.propagateTimeout)

	// other node should has fewer peers.
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 2 {
		t.Fatalf("expected 2 server number")
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 2 {
		t.Fatalf("expected 2 server number")
	}
}

func TestRaft_RemoveLeader(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()

	// wait for has 2 follower.
	var followers []*Raft
	limit := time.Now().Add(c.longStopTimeout)
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.propagateTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected tow follower: %d", len(followers))
	}

	// remove the leader.
	future := leader.RemoveServer(leader.localID, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// wait a while, should has a new leader.
	time.Sleep(c.propagateTimeout)

	newLeader := c.Leader()

	// wait for log application.
	time.Sleep(c.propagateTimeout)
	if newLeader == leader {
		t.Fatalf("removed leader is still leader")
	}

	// other node should have fewer peers.
	if configuration := c.getConfiguration(newLeader); len(configuration.Servers) != 2 {
		t.Fatalf("expected 2 server number")
	}

	// old leader should be shutdown.
	if leader.State() != Shutdown {
		t.Fatalf("old leader should be shutdown")
	}
}

func TestRaft_RemoveLeader_NoShutdown(t *testing.T) {
	// make a cluster.
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false
	c := MakeCluster(3, t, conf)
	defer c.Close()

	// get the leader.
	c.Followers()
	leader := c.Leader()

	// remove the leader.
	for i := byte(0); i < 100; i++ {
		if i == 80 {
			future := leader.RemoveServer(leader.localID, 0, 0)
			if err := future.Error(); err != nil {
				t.Fatalf("remove leader failed, err: %v", err)
			}
		}
		future := leader.Apply([]byte{i}, 0)
		if i > 80 {
			if err := future.Error(); err == nil || err != ErrNotLeader {
				t.Fatalf("future entries should failed, err: %v", err)
			}
		}
	}

	// wait a while for the new leader.
	time.Sleep(c.propagateTimeout)

	newLeader := c.Leader()

	// wait for log application.
	time.Sleep(c.propagateTimeout)

	// other nodes should have pulled(挤走) the leader.
	configuration := c.getConfiguration(newLeader)
	if len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}
	if hasVote(configuration, leader.localID) {
		t.Fatalf("old leader should no longer have a vote")
	}

	// old leader should be a follower.
	if leader.State() != Follower {
		t.Fatalf("leader should be a follower")
	}

	// old leader should not include itself in its peers.
	configuration = c.getConfiguration(leader)
	if len(configuration.Servers) != 2 {
		t.Fatalf("too many peers")
	}
	if hasVote(configuration, leader.localID) {
		t.Fatalf("old leader should no longer have a vote")
	}

	// other nodes should have the same FMS.
	c.EnsureSame(t)
}

func TestRaft_RemoveFollower_SplitCluster(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(4, t, conf)
	defer c.Close()

	leader := c.Leader()

	// wait to make sure knowledge of the 4th server is known to all the
	// peers.
	numServers := 0
	limit := time.Now().Add(c.longStopTimeout)
	for time.Now().Before(limit) && numServers != 4 {
		time.Sleep(c.propagateTimeout)
		configuration := c.getConfiguration(leader)
		numServers = len(configuration.Servers)
	}
	if numServers != 4 {
		t.Fatalf("Leader should have 4 servers, got %d", numServers)
	}
	c.EnsureSamePeers(t)

	// isolate two of the followers.
	followers := c.Followers()
	if len(followers) != 3 {
		t.Fatalf("Expected 3 followers, got %d", len(followers))
	}
	c.Partition([]ServerAddress{followers[0].localAddr, followers[1].localAddr})

	// 尝试删除与 leader 一起留下的剩余 follower. leader 收不到法定人数的联系后,会转为 follower,
	// 所以不能执行成功.
	future := leader.RemoveServer(followers[2].localID, 0, 0)
	if err := future.Error(); err == nil {
		t.Fatalf("Should not have been able to make peer change")
	}
}

func TestRaft_AddKnownPeer(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// get the leader.
	leader := c.Leader()
	followers := c.GetInState(Follower)

	configReq := &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	startingConfig := configReq.configurations.committed
	startingConfigIdx := configReq.configurations.committedIndex

	// try to add the existing follower.
	future := leader.AddVoter(followers[0].localID, followers[0].localAddr, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("AddVoter() err: %v", err)
	}

	configReq = &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	newConfig := configReq.configurations.committed
	newConfigIdx := configReq.configurations.committedIndex

	if newConfigIdx <= startingConfigIdx {
		t.Fatalf("AddVoter should have written a new config entry, but configurations.commitedIndex still %d", newConfigIdx)
	}
	if !reflect.DeepEqual(newConfig, startingConfig) {
		t.Fatalf("[ERR] AddVoter with existing peer shouldn't have changed config, was %#v, but now %#v", startingConfig, newConfig)
	}
}

func TestRaft_RemoveUnknownPeer(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// get the leader.
	leader := c.Leader()
	configReq := &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	startingConfig := configReq.configurations.committed
	startingConfigIdx := configReq.configurations.committedIndex

	// remove unknown.
	future := leader.RemoveServer(ServerID(NewInmemAddr()), 0, 0)

	// nothing to do, should be a new config entry that's the same as before.
	if err := future.Error(); err != nil {
		t.Fatalf("RemoveServer() err: %v", err)
	}
	configReq = &configurationsFuture{}
	configReq.init()
	leader.configurationsCh <- configReq
	if err := configReq.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	newConfig := configReq.configurations.committed
	newConfigIdx := configReq.configurations.committedIndex
	if newConfigIdx <= startingConfigIdx {
		t.Fatalf("RemoveServer should have written a new config entry, but configurations.commitedIndex still %d", newConfigIdx)
	}
	if !reflect.DeepEqual(newConfig, startingConfig) {
		t.Fatalf("[ERR] RemoveServer with unknown peer shouldn't of changed config, was %#v, but now %#v", startingConfig, newConfig)
	}
}

func TestRaft_SnapshotRestore(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// commit a lot of logs.
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// wait for the last future to apply.
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// take a snapshot.
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// check for snapshot.
	snaps, _ := leader.snapshotStore.List()
	if len(snaps) != 1 {
		t.Fatalf("should have a snapshot")
	}
	snap := snaps[0]

	// log should be trimmed.
	if idx, _ := leader.logStore.FirstIndex(); idx != snap.Index-conf.TrailingLogs+1 {
		t.Fatalf("should trim logs to %d: but is %d", snap.Index-conf.TrailingLogs+1, idx)
	}

	// shutdown.
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// restart the Raft.
	r := leader
	// can't just reuse the old transport as it will be closed.
	_, trans2 := NewInmemTransport(r.transport.LocalAddr())
	cfg := r.config()
	r, err := NewRaft(&cfg, &MockFSM{}, r.logStore, r.stableStore, r.snapshotStore, trans2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	c.rafts[0] = r

	// we should have restored from the snapshot!
	if last := r.getLastApplied(); last != snap.Index {
		t.Fatalf("bad last index: %d, expecting %d", last, snap.Index)
	}
}

func TestRaft_NoRestoreOnStart(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	conf.NoSnapshotRestoreOnStart = true
	c := MakeCluster(1, t, conf)

	// commit a lot of things.
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// wait for the last future to apply.
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// take a snapshot.
	snapFuture := leader.Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// shutdown.
	shutdown := leader.Shutdown()
	if err := shutdown.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	_, trans := NewInmemTransport(leader.localAddr)
	newFSM := &MockFSM{}
	cfg := leader.config()
	_, err := NewRaft(&cfg, newFSM, leader.logStore, leader.stableStore, leader.snapshotStore, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(newFSM.logs) != 0 {
		t.Fatalf("expected empty FSM, got %v", newFSM)
	}
}

func TestRaft_AutoSnapshot(t *testing.T) {
	conf := inmemConfig(t)
	conf.SnapshotInterval = conf.CommitTimeout * 2
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// Commit a lot of things
	leader := c.Leader()
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// Wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for a snapshot to happen
	time.Sleep(c.propagateTimeout)

	// Check for snapshot
	if snaps, _ := leader.snapshotStore.List(); len(snaps) == 0 {
		t.Fatalf("should have a snapshot")
	}
}

func TestRaft_UserSnapshot(t *testing.T) {
	// Make the cluster.
	conf := inmemConfig(t)
	conf.SnapshotThreshold = 50
	conf.TrailingLogs = 10
	c := MakeCluster(1, t, conf)
	defer c.Close()

	// With nothing committed, asking for a snapshot should return an error.
	leader := c.Leader()
	if userSnapshotErrorsOnNoData {
		if err := leader.Snapshot().Error(); err != ErrNothingNewToSnapshot {
			t.Fatalf("Request for Snapshot failed: %v", err)
		}
	}

	// Commit some things.
	var future Future
	for i := 0; i < 10; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// Now we should be able to ask for a snapshot without getting an error.
	if err := leader.Snapshot().Error(); err != nil {
		t.Fatalf("Request for Snapshot failed: %v", err)
	}

	// Check for snapshot
	if snaps, _ := leader.snapshotStore.List(); len(snaps) == 0 {
		t.Fatalf("should have a snapshot")
	}
}

// snapshotAndRestore 执行快照和恢复序列,并将给定的偏移量应用于快照索引,因此我们可以尝试不同的情况.
func snapshotAndRestore(t *testing.T, offset uint64) {
	conf := inmemConfig(t)
	// 快照操作多表现为文件IO操作,增加超时时间.
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()
	var future Future
	for i := 0; i < 10; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// take a snapshot.
	snap := leader.Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatalf("Request for Snapshot failed: %v", err)
	}

	// commit some things.
	for i := 10; i < 20; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}

	// get the last index before restore.
	preIndex := leader.getLastIndex()

	meta, reader, err := snap.Open()
	meta.Index += offset
	if err != nil {
		t.Fatalf("Snapshot open failed: %v", err)
	}
	defer reader.Close()
	if err = leader.Restore(meta, reader, 5*time.Second); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// 确保索引更新正确. +2 是因为我们使用了一个索引在创建空洞,在 restore 之后
	// 应用了一个 noop 日志.
	var expected uint64
	if meta.Index > preIndex {
		expected = meta.Index + 2
	} else {
		expected = preIndex + 2
	}
	lastIndex := leader.getLastIndex()
	if lastIndex != expected {
		t.Fatalf("Index was not updated correctly: %d vs. %d", lastIndex, expected)
	}

	// 确保日志相同,我们拥有的所有东西都是最初的快照内容,也是我们之后恢复的内容.
	c.EnsureSame(t)
	fsm := getMockFSM(c.fsms[0])
	fsm.Lock()
	if len(fsm.logs) != 10 {
		t.Fatalf("Log length bad: %d", len(fsm.logs))
	}
	for i, log := range fsm.logs {
		expectedStr := []byte(fmt.Sprintf("test%d", i))
		if bytes.Compare(log, expectedStr) != 0 {
			t.Fatalf("Log entry bad: %v", log)
		}
	}
	fsm.Unlock()

	// commit some more things.
	for i := 20; i < 30; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test %d", i)), 0)
	}
	if err = future.Error(); err != nil {
		t.Fatalf("Error Apply new log entries: %v", err)
	}
	c.EnsureSame(t)
}

func TestRaft_UserRestore(t *testing.T) {
	cases := []uint64{
		0,
		1,
		2,

		// Snapshots from the future
		100,
		1000,
		10000,
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case %v", c), func(t *testing.T) {
			snapshotAndRestore(t, c)
		})
	}
}

func TestRaft_SendSnapshotFollower(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	followers := c.Followers()
	leader := c.Leader()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// commit a lot of logs.
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}
	if err := future.Error(); err != nil {
		t.Fatalf("Commit logs err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// snapshot, this will truncate log.
	for _, r := range c.rafts {
		future = r.Snapshot()
		if err := future.Error(); err != nil && err != ErrNothingNewToSnapshot {

		}
	}

	c.FullyConnect()

	c.EnsureSame(t)
}

func TestRaft_SendSnapshotAndLogsFollower(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 10
	c := MakeCluster(3, t, conf)
	defer c.Close()

	followers := c.Followers()
	leader := c.Leader()
	behind := followers[0]
	c.Disconnect(behind.localAddr)

	// commit a lot of logs.
	var future Future
	for i := 0; i < 100; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	// snapshot, this will truncate logs.
	for _, r := range c.rafts {
		future = r.Snapshot()
		// the disconnected node will have nothing to snapshot, so that's expected.
		if err := future.Error(); err != nil && err != ErrNothingNewToSnapshot {
			t.Fatalf("err: %v", err)
		}
	}

	// commit more logs past the snapshot.
	for i := 100; i < 200; i++ {
		future = leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// wait for the last future to apply
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	} else {
		t.Logf("[INFO] Finished apply without behind follower")
	}

	c.FullyConnect()

	c.EnsureSame(t)
}

func TestRaft_ReJoinFollower(t *testing.T) {
	conf := inmemConfig(t)
	conf.ShutdownOnRemove = false
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()

	// wait for have 2 followers.
	var followers []*Raft
	limit := time.Now().Add(c.longStopTimeout)
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers: %v", followers)
	}

	// remove a follower.
	follower := followers[0]
	future := leader.RemoveServer(follower.localID, 0, 0)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// other node should has fewer peer.
	time.Sleep(c.propagateTimeout)
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers: %v", configuration)
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 2 {
		t.Fatalf("too many peers: %v", configuration)
	}

	// 获得 leader.不能使用 GetInState() 方法,因为被移除的 follower 会不断发起选举,会产生 RequestVote RPCs,
	// 从而导致方法不能正常运行.
	limit = time.Now().Add(c.longStopTimeout)
	var leaders []*Raft
	for time.Now().Before(limit) && len(leaders) != 1 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		leaders, _ = c.pollState(Leader)
	}
	if len(leaders) != 1 {
		t.Fatalf("expected a leader")
	}
	leader = leaders[0]

	// ReJoin! follower 会具有较大的任期,会导致 leader 下台,进入新一轮选举.
	// 但 leader 的 lastLogIndex 较大,会作为新的 leader.
	future = leader.AddVoter(follower.localID, follower.localAddr, 0, 0)
	if err := future.Error(); err != nil && err != ErrLeadershipLost {
		t.Fatalf("err: %v", err)
	}

	// 等待集群重新稳定.
	leader = c.Leader()
	if configuration := c.getConfiguration(leader); len(configuration.Servers) != 3 {
		t.Fatalf("missing peers: %v", configuration)
	}
	if configuration := c.getConfiguration(followers[1]); len(configuration.Servers) != 3 {
		t.Fatalf("missing peers: %v", configuration)
	}

	// should be a follower now.
	if follower.State() != Follower {
		t.Fatalf("bad state: %v", follower.State())
	}
}

func TestRaft_LeaderLeaseExpire(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	leader := c.Leader()

	limit := time.Now().Add(c.longStopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 1 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 1 {
		t.Fatalf("expected a followers: %v", followers)
	}

	// disconnect.
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// watch the leaderCh.
	timeout := time.After(c.conf.LeaderLeaseTimeout * 2)
LOOP:
	for {
		select {
		case v := <-leader.LeaderCh():
			if !v {
				break LOOP
			}
		case <-timeout:
			t.Fatalf("timeout stepping down as leader")
		}
	}

	// ensure the last contact of leader is nonzero.
	if leader.LastContact().IsZero() {
		t.Fatalf("expected non-zero contact time")
	}

	// should be no leader.
	// 此处可以调用该方法来获取稳定的 leader,因为 RequestVote RPCs 是接收方才会产生的 observation,
	// 由于已经断掉了 Transport 连接,RequestVote RPCs 是不会被发出的.
	if len(c.GetInState(Leader)) != 0 {
		t.Fatalf("expected step down")
	}

	last := follower.LastContact()
	time.Sleep(c.propagateTimeout)

	// check the last contact not change.
	if last != follower.LastContact() {
		t.Fatalf("unexpected further contact")
	}

	// ensure both have cleared their leader.
	if l := leader.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
	if l := follower.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
}

func TestRaft_Barrier(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()

	// commit a lot of things.
	for i := 0; i < 100; i++ {
		leader.Apply([]byte(fmt.Sprintf("test%d", i)), 0)
	}

	// wait for a barrier complete.
	barrier := leader.Barrier(0)

	// wait for the barrier future to apply.
	if err := barrier.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	c.EnsureSame(t)
	if len(getMockFSM(c.fsms[0]).logs) != 100 {
		t.Fatalf(fmt.Sprintf("Bad log length: %d", len(getMockFSM(c.fsms[0]).logs)))
	}
}

func TestRaft_VerifyLeader(t *testing.T) {
	c := MakeCluster(3, t, nil)
	defer c.Close()

	leader := c.Leader()

	// verify we are leader.
	verify := leader.VerifyLeader()

	// wait for the verify to apply.
	if err := verify.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_VerifyLeader_Single(t *testing.T) {
	c := MakeCluster(1, t, nil)
	defer c.Close()

	leader := c.Leader()

	// verify we are leader.
	verify := leader.VerifyLeader()

	// wait for the verify to apply.
	if err := verify.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestRaft_VerifyLeader_Fail(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(2, t, conf)
	defer c.Close()

	// 清除 channel 中关于领导者选举的通知.
	leader := c.Leader()
	<-leader.LeaderCh()

	followers := c.Followers()

	// force follower to different term.
	follower := followers[0]
	follower.setCurrentTerm(follower.getCurrentTerm() + 1)

	// wait for the leader to step down.
	select {
	case v := <-leader.LeaderCh():
		if v {
			t.Fatalf("expected the leader to step down")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		t.Fatalf("timeout waiting for leader to step down")
	}

	// verify we are leader ?
	future := leader.VerifyLeader()
	if err := future.Error(); err != nil && err != ErrNotLeader && err != ErrLeadershipLost {
		t.Fatalf("err: %v", err)
	}

	// ensure the known leader is cleared.
	if l := leader.Leader(); l != "" {
		t.Fatalf("bad: %v", l)
	}
}

func TestRaft_VerifyLeader_PartialConnect(t *testing.T) {
	conf := inmemConfig(t)
	c := MakeCluster(3, t, conf)
	defer c.Close()

	leader := c.Leader()

	limit := time.Now().Add(c.longStopTimeout)
	var followers []*Raft
	for time.Now().Before(limit) && len(followers) != 2 {
		c.WaitEvent(nil, c.conf.CommitTimeout)
		followers = c.GetInState(Follower)
	}
	if len(followers) != 2 {
		t.Fatalf("expected two followers but got: %v", followers)
	}

	// force partial disconnect.
	follower := followers[0]
	t.Logf("[INFO] Disconnecting %v", follower)
	c.Disconnect(follower.localAddr)

	// verify we are leader.
	future := leader.VerifyLeader()
	// votes meets to quorum size.
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
}

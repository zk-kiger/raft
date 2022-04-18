package raft

import (
	"bytes"
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

var (
	userSnapshotErrorsOnNoData = true
)

// inmemConfig Return configurations optimized for in-memory.
func inmemConfig(t *testing.T) *Config {
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.Logger = newTestLogger(t)
	return conf
}

// MockFSM 实现 FSM 接口,顺序存储日志.
type MockFSM struct {
	sync.Mutex
	logs           [][]byte
	configurations []Configuration
}

// MockFSMConfigStore 用于存储配置更改的 FSM.
type MockFSMConfigStore struct {
	FSM
}

func getMockFSM(fsm FSM) *MockFSM {
	switch f := fsm.(type) {
	case *MockFSM:
		return f
	case *MockFSMConfigStore:
		return f.FSM.(*MockFSM)
	}
	return nil
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log *Log) interface{} {
	m.Lock()
	defer m.Unlock()
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

func (m *MockFSM) Snapshot() (FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockFSM) Logs() [][]byte {
	m.Lock()
	defer m.Unlock()
	return m.logs
}

func (m *MockFSMConfigStore) StoreConfiguration(index uint64, config Configuration) {
	mm := m.FSM.(*MockFSM)
	mm.Lock()
	defer mm.Unlock()
	mm.configurations = append(mm.configurations, config)
}

func (m *MockSnapshot) Persist(sink SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (m *MockSnapshot) Release() {
}

// This can be used as the destination for a logger and it'll
// map them into calls to testing.T.Log, so that you only see
// the logging for failed tests.
type testLoggerAdapter struct {
	t      *testing.T
	prefix string
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	if a.prefix != "" {
		l := a.prefix + ": " + string(d)
		a.t.Log(l)
		return len(l), nil
	}

	a.t.Log(string(d))
	return len(d), nil
}

func newTestLogger(t *testing.T) hclog.Logger {
	return newTestLoggerWithPrefix(t, "")
}

// newTestLoggerWithPrefix 返回一个可用于测试的 Logger.前缀将作为 Logger 的名称添加.
//
// 如果使用 -v(详细模式,或 -json 表示详细)运行测试,则日志输出将直接转到 stderr.
// 如果测试以常规的"quiet"模式运行,日志将被发送到 t.Log,以便日志仅在测试失败时出现.
func newTestLoggerWithPrefix(t *testing.T, prefix string) hclog.Logger {
	if testing.Verbose() {
		return hclog.New(&hclog.LoggerOptions{Name: prefix})
	}

	return hclog.New(&hclog.LoggerOptions{
		Name:   prefix,
		Output: &testLoggerAdapter{t: t, prefix: prefix},
	})
}

type cluster struct {
	dirs             []string
	stores           []*InmemStore
	fsms             []FSM
	snaps            []*FileSnapshotStore
	trans            []LoopbackTransport
	rafts            []*Raft
	t                *testing.T
	observationCh    chan Observation
	conf             *Config
	propagateTimeout time.Duration
	longStopTimeout  time.Duration
	logger           hclog.Logger
	startTime        time.Time

	failedLock sync.Mutex
	failedCh   chan struct{}
	failed     bool
}

func (c *cluster) Merge(other *cluster) {
	c.dirs = append(c.dirs, other.dirs...)
	c.stores = append(c.stores, other.stores...)
	c.fsms = append(c.fsms, other.fsms...)
	c.snaps = append(c.snaps, other.snaps...)
	c.trans = append(c.trans, other.trans...)
	c.rafts = append(c.rafts, other.rafts...)
}

// Failf 提供了一个日志功能,使测试失败,以 logger 打印输出,并且不会神秘地吃掉字符串.
// 这可以从 goroutine 安全地调用,但不会立即停止测试.
// failedCh 将被关闭以允许主线程中的阻塞函数检测故障并做出反应.
// 请注意,您应该安排主线程阻塞,直到所有 goroutine 完成,以便使用此函数可靠地使测试失败.
func (c *cluster) Failf(format string, args ...interface{}) {
	c.logger.Error(fmt.Sprintf(format, args...))
	c.t.Fail()
	c.notifyFailed()
}

func (c *cluster) notifyFailed() {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()
	if !c.failed {
		c.failed = true
		close(c.failedCh)
	}
}

// Close 关闭并清理集群.
func (c *cluster) Close() {
	var futures []Future
	for _, r := range c.rafts {
		futures = append(futures, r.Shutdown())
	}

	// 等待关闭.
	limit := time.AfterFunc(c.longStopTimeout, func() {
		// 不能使用 t.FailNow,t.Fatalf, 因为 FailNow 必须从运行测试或基准函数的 goroutine 调用,
		// 而不是从测试期间创建的其他 goroutine 调用.调用 FailNow 不会停止其他 goroutine.
		// 所以 panic.
		panic("timed out waiting for shutdown")
	})
	defer limit.Stop()

	for _, f := range futures {
		if err := f.Error(); err != nil {
			c.t.Fatalf("shutdown future err: %v", err)
		}
	}

	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}

// WaitEventChan 返回一个通道,该通道将在进行观察或发生超时时发出信号.可以设置过滤器来查找特定的观察结果.
// 将超时设置为 0 意味着它将永远等待,直到进行未过滤的观察.
func (c *cluster) WaitEventChan(ctx context.Context, filter FilterFn) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case o, ok := <-c.observationCh:
				if !ok || filter == nil || filter(&o) {
					return
				}
			}
		}
	}()
	return ch
}

// WaitEvent 一直等待,直到进行观察、发生超时或发出测试失败信号.可以设置过滤器来查找特定的观察结果.
// 将 timeout 设置为 0 意味着它将永远等待,直到进行未过滤的观察或发出测试失败的信号.
func (c *cluster) WaitEvent(filter FilterFn, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	eventCh := c.WaitEventChan(ctx, filter)
	select {
	case <-c.failedCh:
		c.t.FailNow()
	case <-eventCh:
	}
}

// WaitForReplication 阻塞,直到集群中的每个 FSM 都具x有给定的长度,或者长时间的健全性检查超时到期.
func (c *cluster) WaitForReplication(fsmLength int) {
	limitCh := time.After(c.longStopTimeout)

CHECK:
	for {
		ctx, cancel := context.WithTimeout(context.Background(), c.conf.CommitTimeout)
		defer cancel()
		ch := c.WaitEventChan(ctx, nil)
		select {
		case <-c.failedCh:
			c.t.FailNow()

		case <-limitCh:
			c.t.Fatalf("timeout waiting for replication")

		case <-ch:
			for _, fsmRaw := range c.fsms {
				fsm := getMockFSM(fsmRaw)
				fsm.Lock()
				num := len(fsm.logs)
				fsm.Unlock()
				if num != fsmLength {
					continue CHECK
				}
			}
			return
		}
	}
}

// pollState 拍摄集群状态的快照.这可能不稳定,因此在等待集群达到特定状态时使用 GetInState() 应用一些额外的检查.
func (c *cluster) pollState(s RaftState) ([]*Raft, uint64) {
	var highestTerm uint64
	in := make([]*Raft, 0, 1)
	for _, r := range c.rafts {
		if r.State() == s {
			in = append(in, r)
		}
		term := r.getCurrentTerm()
		if term > highestTerm {
			highestTerm = term
		}
	}
	return in, highestTerm
}

// GetInState 轮询集群的状态并尝试识别它何时进入给定状态.
func (c *cluster) GetInState(s RaftState) []*Raft {
	c.logger.Info("starting stability test", "raft-state", s)
	limitCh := time.After(c.longStopTimeout)

	// 选举应该在 2 * max(HeartbeatTimeout, ElectionTimeout) 之后完成,
	// 因为随机计时器在 1 x 间隔 ... 2 x 间隔内到期.我们增加了一点传播延迟.
	// 如果选举失败(例如因为两个选举同时开始),我们将通过我们的观察者通道获得一些指示不同状态的信息
	// (即其中一个节点将转移到候选状态),这将重置计时器.
	//
	// 由于实现的特殊性,它实际上可以是 3 x 超时.
	timeout := c.conf.HeartbeatTimeout
	if timeout < c.conf.ElectionTimeout {
		timeout = c.conf.ElectionTimeout
	}
	timeout = 2*timeout + c.conf.CommitTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// 等到我们有一个稳定的状态切片.每次我们看到一个观察状态发生了变化,
	// 重新检查它,如果它发生了变化,重新启动计时器.
	var pollStartTime = time.Now()
	for {
		inState, highestTerm := c.pollState(s)
		inStateTime := time.Now()

		// 有时这个程序会在 Raft 启动之前很早就被调用.即使没有人开始选举.我们也会超时.
		// 因此,如果使用的最高 term 为零,我们知道没有 raft 进程尚未发出 RequestVote,我们设置了很长的时间.
		// 当我们听到第一个 RequestVote 时,这是固定的,此时我们重置计时器.
		if highestTerm == 0 {
			timer.Reset(c.longStopTimeout)
		} else {
			timer.Reset(timeout)
		}

		// 每当我们观察到 RequestVote 时,过滤器就会唤醒.
		filter := func(ob *Observation) bool {
			switch ob.Data.(type) {
			case RaftState:
				return true
			case RequestVoteRequest:
				return true
			default:
				return false
			}
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		eventCh := c.WaitEventChan(ctx, filter)
		select {
		case <-c.failedCh:
			c.t.FailNow()

		case <-limitCh:
			c.t.Fatalf("timeout waiting for stable %s state", s)

		case <-eventCh:
			c.logger.Debug("resetting stability timeout")

		case t, ok := <-timer.C:
			if !ok {
				c.t.Fatalf("timer channel errored")
			}

			c.logger.Info(fmt.Sprintf("stable state for %s reached at %s (%d nodes), %s from start of poll, %s from cluster start. Timeout at %s, %s after stability",
				s, inStateTime, len(inState), inStateTime.Sub(pollStartTime), inStateTime.Sub(c.startTime), t, t.Sub(inStateTime)))
			return inState
		}
	}
}

// Leader 等待集群选举领导者并保持稳定状态.
func (c *cluster) Leader() *Raft {
	c.t.Helper()
	leaders := c.GetInState(Leader)
	if len(leaders) != 1 {
		c.t.Fatalf("expected one leader: %v", leaders)
	}
	return leaders[0]
}

// Followers 等待集群有 N-1 个追随者并保持稳定状态.
func (c *cluster) Followers() []*Raft {
	expFollowers := len(c.rafts) - 1
	followers := c.GetInState(Follower)
	if len(followers) != expFollowers {
		c.t.Fatalf("timeout waiting for %d followers (followers are %v)", expFollowers, followers)
	}
	return followers
}

// Partition 保持给定节点之间的网络联系,孤立它们与集群其他成员.
func (c *cluster) Partition(far []ServerAddress) {
	c.logger.Debug("partitioning", "addresses", far)

	// 筛选出两个分区的节点集合.
	near := make(map[ServerAddress]struct{})
OUTER:
	for _, t := range c.trans {
		l := t.LocalAddr()
		for _, a := range far {
			if l == a {
				continue OUTER
			}
		}
		near[l] = struct{}{}
	}

	// 断开 near 和 far 两个分区中节点的联系.
	for _, t := range c.trans {
		l := t.LocalAddr()
		if _, ok := near[l]; ok {
			for _, a := range far {
				t.Disconnect(a)
			}
		} else {
			for a := range near {
				t.Disconnect(a)
			}
		}
	}
}

// EnsureLeader 检查所有的 raft 节点,判断给定的 expect 是否是 leader.
func (c *cluster) EnsureLeader(t *testing.T, expect ServerAddress) {
	// 我们假设 c.Leader() 已经被调用;现在检查所有的 raft 认为 leader 是正确的.
	fail := false
	for _, r := range c.rafts {
		leader := ServerAddress(r.Leader())
		if leader != expect {
			if leader == "" {
				leader = "[none]"
			}
			if expect == "" {
				c.logger.Error("peer sees incorrect leader", "peer", r, "leader", leader, "expected-leader", "[none]")
			} else {
				c.logger.Error("peer sees incorrect leader", "peer", r, "leader", leader, "expected-leader", expect)
			}
			fail = true
		}
	}
	if fail {
		t.Fatalf("at least one peer has the wrong notion of leader")
	}
}

// EnsureSame 确保所有的 fsm 拥有相同的内容.
func (c *cluster) EnsureSame(t *testing.T) {
	limit := time.Now().Add(c.longStopTimeout)
	first := getMockFSM(c.fsms[0])

CHECK:
	first.Lock()
	for i, fsmRaw := range c.fsms {
		fsm := getMockFSM(fsmRaw)
		if i == 0 {
			continue
		}
		fsm.Lock()

		if len(first.logs) != len(fsm.logs) {
			fsm.Unlock()
			if time.Now().After(limit) {
				t.Fatalf("FSM log length mismatch: %d %d",
					len(first.logs), len(fsm.logs))
			} else {
				goto WAIT
			}
		}

		for idx := 0; idx < len(first.logs); idx++ {
			if bytes.Compare(first.logs[idx], fsm.logs[idx]) != 0 {
				fsm.Unlock()
				if time.Now().After(limit) {
					t.Fatalf("FSM log mismatch at index %d", idx)
				} else {
					goto WAIT
				}
			}
		}
		if len(first.configurations) != len(fsm.configurations) {
			fsm.Unlock()
			if time.Now().After(limit) {
				t.Fatalf("FSM configuration length mismatch: %d %d",
					len(first.logs), len(fsm.logs))
			} else {
				goto WAIT
			}
		}

		for idx := 0; idx < len(first.configurations); idx++ {
			if !reflect.DeepEqual(first.configurations[idx], fsm.configurations[idx]) {
				fsm.Unlock()
				if time.Now().After(limit) {
					t.Fatalf("FSM configuration mismatch at index %d: %v, %v", idx, first.configurations[idx], fsm.configurations[idx])
				} else {
					goto WAIT
				}
			}
		}
		fsm.Unlock()
	}

	first.Unlock()
	return

WAIT:
	first.Unlock()
	c.WaitEvent(nil, c.conf.CommitTimeout)
	goto CHECK
}

// getConfiguration 返回给定 Raft 实例的配置,如果有错误则测试失败.
func (c *cluster) getConfiguration(r *Raft) Configuration {
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		c.t.Fatalf("failed to get configuration: %v", err)
		return Configuration{}
	}

	return future.Configuration()
}

// EnsureSamePeers 确保所有的 raft 节点都拥有相同的节点配置.
func (c *cluster) EnsureSamePeers(t *testing.T) {
	limit := time.Now().Add(c.longStopTimeout)
	peerSet := c.getConfiguration(c.rafts[0])

CHECK:
	for i, raft := range c.rafts {
		if i == 0 {
			continue
		}

		otherSet := c.getConfiguration(raft)
		if !reflect.DeepEqual(peerSet, otherSet) {
			if time.Now().After(limit) {
				t.Fatalf("peer mismatch: %+v %+v", peerSet, otherSet)
			} else {
				goto WAIT
			}
		}
	}
	return

WAIT:
	c.WaitEvent(nil, c.conf.CommitTimeout)
	goto CHECK
}

// MakeClusterOpts 构建集群可用配置项.
type MakeClusterOpts struct {
	Peers           int
	Bootstrap       bool
	Conf            *Config
	ConfigStoreFSM  bool
	MakeFSMFunc     func() FSM
	LongStopTimeout time.Duration
}

// makeCluster 返回给定配置和节点数量组成的集群.
// 如果 Bootstrap 为 true,集群节点将会在启动之前相互了解,否则节点的 transport 被建立,但节点之间尚未配置,
// 后续可以通过 BootstrapCluster api 使节点配置被了解.
func makeCluster(t *testing.T, opts *MakeClusterOpts) *cluster {
	if opts.Conf == nil {
		opts.Conf = inmemConfig(t)
	}

	c := &cluster{
		observationCh: make(chan Observation, 1024),
		conf:          opts.Conf,
		// 传播延时需要 2 次心跳,可能会加上触发提交心跳的时间.
		propagateTimeout: opts.Conf.HeartbeatTimeout*2 + opts.Conf.CommitTimeout,
		longStopTimeout:  5 * time.Second,
		logger:           newTestLoggerWithPrefix(t, "cluster"),
		failedCh:         make(chan struct{}),
	}
	if opts.LongStopTimeout > 0 {
		c.longStopTimeout = opts.LongStopTimeout
	}

	c.t = t
	var configuration Configuration

	// 建立 Store 和 transport.
	for i := 0; i < opts.Peers; i++ {
		dir, err := ioutil.TempDir("", "raft")
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		store := NewInmemStore()
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		if opts.ConfigStoreFSM {
			c.fsms = append(c.fsms, &MockFSMConfigStore{
				FSM: &MockFSM{},
			})
		} else {
			var fsm FSM
			if opts.MakeFSMFunc != nil {
				fsm = opts.MakeFSMFunc()
			} else {
				fsm = &MockFSM{}
			}
			c.fsms = append(c.fsms, fsm)
		}

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		addr, trans := NewInmemTransport("")
		c.trans = append(c.trans, trans)
		localID := ServerID(fmt.Sprintf("server-%s", addr))
		if opts.Conf.ProtocolVersion < 3 {
			localID = ServerID(addr)
		}
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       localID,
			Address:  addr,
		})
	}

	// 将 transport 连接起来.
	c.FullyConnect()

	// 创建所有 raft.
	c.startTime = time.Now()
	for i := 0; i < opts.Peers; i++ {
		logs := c.stores[i]
		store := c.stores[i]
		snap := c.snaps[i]
		trans := c.trans[i]

		peerConf := opts.Conf
		peerConf.LocalID = configuration.Servers[i].ID
		peerConf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[i].ID))

		if opts.Bootstrap {
			err := BootstrapCluster(peerConf, logs, store, snap, trans, configuration)
			if err != nil {
				t.Fatalf("BootstrapCluster failed: %v", err)
			}
		}

		raft, err := NewRaft(peerConf, c.fsms[i], logs, store, snap, trans)
		if err != nil {
			t.Fatalf("NewRaft failed: %v", err)
		}

		raft.RegisterObserver(NewObserver(c.observationCh, false, nil))

		c.rafts = append(c.rafts, raft)
	}

	return c
}

func MakeCluster(n int, t *testing.T, conf *Config) *cluster {
	return makeCluster(t, &MakeClusterOpts{
		Peers:     n,
		Bootstrap: true,
		Conf:      conf,
	})
}

func MakeClusterNoBootstrap(n int, t *testing.T, conf *Config) *cluster {
	return makeCluster(t, &MakeClusterOpts{
		Peers: n,
		Conf:  conf,
	})
}

func FileSnapTest(t *testing.T) (string, *FileSnapshotStore) {
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	snap, err := NewFileSnapshotStoreWithLogger(dir, 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	snap.noSync = true
	return dir, snap
}

// FullyConnect 将所有的 transport 连接起来.
func (c *cluster) FullyConnect() {
	c.logger.Debug("fully connecting")
	for i, t1 := range c.trans {
		for j, t2 := range c.trans {
			if i != j {
				t1.Connect(t2.LocalAddr(), t2)
				t2.Connect(t1.LocalAddr(), t1)
			}
		}
	}
}

// Disconnect 关闭给定地址的所有 Transport.
func (c *cluster) Disconnect(a ServerAddress) {
	c.logger.Debug("disconnecting", "address", a)
	for _, t := range c.trans {
		if t.LocalAddr() == a {
			t.DisconnectAll()
		} else {
			t.Disconnect(a)
		}
	}
}

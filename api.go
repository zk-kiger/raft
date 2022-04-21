package raft

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrNotLeader 当某个操作不能由 follower/candidate 时返回.
	ErrNotLeader = errors.New("node is not the leader")

	// ErrLeadershipLost 当 leader 因为在进程中被废止而未能提交日志条目时返回.
	ErrLeadershipLost = errors.New("leadership lost while committing log")

	// ErrAbortedByRestore is returned when a leader fails to commit a log
	// entry because it's been superseded by a user snapshot restore.
	ErrAbortedByRestore = errors.New("snapshot restored while committing log")

	// ErrRaftShutdown 当操作请求不活跃的 Raft 时返回.
	ErrRaftShutdown = errors.New("raft already is shutdown")

	// ErrNothingNewToSnapshot is returned when trying to create a snapshot
	// but there's nothing new committed to the FSM since we started.
	ErrNothingNewToSnapshot = errors.New("nothing new to snapshot")

	// ErrCantBootstrap is returned when attempt is made to bootstrap a
	// cluster that already has state present.
	ErrCantBootstrap = errors.New("bootstrap only works on new clusters")

	// ErrEnqueueTimeout is returned when a command fails due to a timeout.
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")
)

// Raft implements a Raft node.
type Raft struct {
	raftState

	protocolVersion ProtocolVersion

	// applyCh 异步地将 logs 通知给 leader,后续进行复制、应用 fsm.
	applyCh chan *logFuture

	// conf 存储要使用的当前配置.这是最新提供的,所有配置值的读取都应该使用
	// config() helper 方法来安全地读取它.
	conf atomic.Value

	// confReloadMu 确保一次只有一个线程加载配置,因此需要 read-modify-write 原子性.
	// 没有必要给 read 进行并发安全操作, read 操作可以调用 config() 读取配置.
	confReloadMu sync.Mutex

	// FSM 有限状态机,用于客户端应用命令记录状态.
	fsm FSM

	// fsmMutateCh 用于向 FSM 发送 state-changing 更新.
	fsmMutateCh chan interface{}

	// fsmSnapshotCh 用于触发正在生成的新快照.
	fsmSnapshotCh chan *reqSnapshotFuture

	// lastContact 上一次联系 leader 的时间.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	// Leader is the current cluster leader
	leader     ServerAddress
	leaderLock sync.RWMutex

	// leaderCh 用于在 leadership change 时被通知.
	leaderCh chan bool

	// Raft 节点为 Leader 时使用.
	leaderState leaderState

	// localID 记录自己的 id,避免发送 RPC 给自己.
	localID   ServerID
	localAddr ServerAddress

	logger hclog.Logger

	// logStore 提供持久化的存储.
	logStore LogStore

	// 请求 leader 进行配置更改.
	configurationChangeCh chan *configurationChangeFuture

	// 从 logs/snapshot 中追踪最新的配置和最新的已提交配置.
	configurations configurations

	// 保存最新配置的副本,可以独立于主循环读取.
	latestConfiguration atomic.Value

	// rpcCh 用于接收来自 transport layer 的 RPC.
	rpcCh <-chan RPC

	// Shutdown 用于退出,使用锁防止并发退出.
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// snapshotStore 用于存储和获取快照.
	snapshotStore SnapshotStore

	// userSnapshotCh 用于用户触发的快照.
	userSnapshotCh chan *userSnapshotFuture

	// userRestoreCh 用于用户触发的外部快照恢复.
	userRestoreCh chan *userRestoreFuture

	// stableStore 为 raftState 中的许多字段(持久化状态)提供了稳定的存储.
	stableStore StableStore

	// transport 用于 raft-node 之间通信.
	transport Transport

	// verifyCh 用于向主线程异步发送验证 future 以验证我们仍然是 leader.
	verifyCh chan *verifyFuture

	// configurationsCh 用于从主线程外部安全地获取配置数据.
	configurationsCh chan *configurationsFuture

	// bootstrapCh 用于尝试执行来自外部的 initial bootstrap configuration.
	bootstrapCh chan *bootstrapFuture

	// observers 用于记录注册的观察者.
	// 引入观察者模式,方便单机多节点检测.
	observersLock sync.RWMutex
	observers     map[uint64]*Observer
}

// BootstrapCluster 使用给定的集群配置初始化服务器的存储.
// 这应该只在集群开始时在配置相同的所有 Voter 服务器上调用.无需引导 Nonvoter 和 Staging 服务器.
func BootstrapCluster(conf *Config, logs LogStore, stable StableStore,
	snaps SnapshotStore, trans Transport, configuration Configuration) error {
	// 验证 Raft 配置.
	if err := ValidateConfig(conf); err != nil {
		return err
	}

	// 测试集群成员配置.
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// 确保集群在一个初始的状态(clean state).
	hasState, err := HasExistingState(logs, stable, snaps)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	}
	if hasState {
		return ErrCantBootstrap
	}

	// 设置 current term 为 1.
	if err = stable.SetUint64(KeyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// 追加 configuration entry 到 logStore.
	entry := &Log{
		Type:  LogConfiguration,
		Index: 1,
		Term:  1,
	}
	entry.Data = EncodeConfiguration(configuration)
	if err = logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}

	return nil
}

// RecoverCluster 用于手动强制执行新配置,以便从无法恢复当前配置的仲裁损失中恢复,例如当多个服务器同时死亡时.
// 这通过读取该服务器的所有当前状态,使用提供的配置创建快照,然后截断 Raft 日志来工作.
// 这是在不实际更改日志以插入任何新条目的情况下强制给定配置的唯一安全方法,这可能会导致与具有不同状态的其他服务器发生冲突.
//
// 警告！这个操作隐式提交了 Raft 日志中的所有条目,所以总的来说这是一个非常不安全的操作.
// 如果你丢失了其他服务器并且正在执行手动恢复,那么你也丢失了提交信息,所以这可能是你能做的最好的事情,但你应该知道,
// 调用它可能会导致 Raft 日志条目在被复制但尚未被提交的过程中被提交.
//
// 请注意,此处传递的 FSM 用于快照操作,并将处于应用程序不应使用的状态.请务必丢弃此 FSM 和任何关联状态,
// 并在稍后调用 NewRaft 时提供一个新状态.
// 对于 Transport 也需要重置新状态,如果使用旧的状态,也许会收到一些非预期的操作.
//
// 恢复集群的典型方法是关闭所有服务器,然后使用相同配置在每台服务器上运行 RecoverCluster.
// 当集群重新启动时,应该会发生选举,然后 Raft 将恢复正常运行.如果希望让特定服务器成为领导者,这可以用于注入新配置,
// 将该服务器作为唯一投票者,然后使用常用 API 加入其他新的干净状态对等服务器,以恢复集群进入已知状态.
func RecoverCluster(conf *Config, fsm FSM, logs LogStore, stable StableStore,
	snaps SnapshotStore, trans Transport, configuration Configuration) error {
	// 验证 Raft 配置.
	if err := ValidateConfig(conf); err != nil {
		return err
	}

	// 测试集群成员配置.
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// 如果没有现有状态,则拒绝恢复.这很可能表明操作员错误,他期望数据在这里,然而并非如此.
	// 通过拒绝,我们来强迫他们通过引导程序显式地重新启动集群,而不是悄悄地启动一个新的集群.
	if hasState, err := HasExistingState(logs, stable, snaps); err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	} else if !hasState {
		return fmt.Errorf("refused to recover cluster with no initial state, this is probably an operator error")
	}

	// 尝试恢复一些找到的快照, newest -> oldest.
	var (
		snapshotIndex  uint64
		snapshotTerm   uint64
		snapshots, err = snaps.List()
	)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	for _, snapshot := range snapshots {
		var source io.ReadCloser
		_, source, err = snaps.Open(snapshot.ID)
		if err != nil {
			// 跳过当前快照重试下一个,我们会检测如果没有任何快照能被打开.
			continue
		}

		err = fsm.Restore(source)
		source.Close()
		if err != nil {
			// 跳过当前快照重试下一个.
			continue
		}

		snapshotIndex = snapshot.Index
		snapshotTerm = snapshot.Term
		break
	}
	if len(snapshots) > 0 && (snapshotIndex == 0 || snapshotTerm == 0) {
		return fmt.Errorf("failed to restore any of the available snapshots")
	}

	// 在回放日志条目之前,快照是最有效的数据点.
	lastIndex := snapshotIndex
	lastTerm := snapshotTerm

	// 应用快照之后的日志条目.
	lastLogIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to find last log: %v", err)
	}
	for idx := snapshotIndex + 1; idx <= lastLogIndex; idx++ {
		var entry Log
		if err = logs.GetLog(idx, &entry); err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", idx, err)
		}
		if entry.Type == LogCommand {
			_ = fsm.Apply(&entry)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	// 创建一个新的快照,将配置放入,就好像它从索引 1 处开始.
	snapshot, err := fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("failed to snapshot FSM: %v", err)
	}
	version := getSnapshotVersion(conf.ProtocolVersion)
	sink, err := snaps.Create(version, lastIndex, lastTerm, configuration, 1, trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err = snapshot.Persist(sink); err != nil {
		return fmt.Errorf("failed to persist snapshot: %v", err)
	}
	if err = sink.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	// 压缩日志,就不会收到任何可能存在的配置更改的影响.
	firstLogIndex, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}
	if err = logs.DeleteRange(firstLogIndex, lastLogIndex); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}

	return nil
}

// HasExistingState 如果服务器具有任何现有的状态(log,current term or snapshot),则返回 true.
func HasExistingState(logs LogStore, stable StableStore, snaps SnapshotStore) (bool, error) {
	// 确保没有 current term.
	currentTerm, err := stable.GetUint64(KeyCurrentTerm)
	if err == nil {
		if currentTerm > 0 {
			return true, nil
		}
	} else {
		if err.Error() != "not found" {
			return false, fmt.Errorf("failed to read current term: %v", err)
		}
	}

	// 确保存在空的日志.
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return false, fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastIndex > 0 {
		return true, nil
	}

	// 确保没有快照.
	snapshots, err := snaps.List()
	if err != nil {
		return false, fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) > 0 {
		return true, nil
	}

	return false, nil
}

// NewRaft 构建一个新的 raft-node.它需要一个配置,以及所需各种接口的实现.
// 如果存在旧状态,例如 快照、日志等等,都会在创建节点时恢复.
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (*Raft, error) {
	// 验证配置合理性.
	if err := ValidateConfig(conf); err != nil {
		return nil, err
	}

	// 确保存在 LogOutput.
	var logger hclog.Logger
	if conf.Logger != nil {
		logger = conf.Logger
	} else {
		if conf.LogOutput == nil {
			conf.LogOutput = os.Stderr
		}
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft",
			Level:  hclog.LevelFromString(conf.LogLevel),
			Output: conf.LogOutput,
		})
	}

	// 尝试恢复 current term.
	currentTerm, err := stable.GetUint64(KeyCurrentTerm)
	if err != nil && err.Error() != ErrKeyNotFound.Error() {
		return nil, fmt.Errorf("failed to restore the current term: %v", err)
	}

	// 读取最后一条日志的索引.
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	// 获取最后一条的日志条目.
	var lastLog Log
	if lastIndex > 0 {
		if err = logs.GetLog(lastIndex, &lastLog); err != nil {
			return nil, fmt.Errorf("failed to get last log at index %d: %v", lastIndex, err)
		}
	}

	// buffer applyCh to MaxAppendEntries.
	applyCh := make(chan *logFuture)
	if conf.BatchApplyCh {
		applyCh = make(chan *logFuture, conf.MaxAppendEntries)
	}

	// make raft struct.
	r := &Raft{
		protocolVersion:       conf.ProtocolVersion,
		applyCh:               applyCh,
		fsm:                   fsm,
		fsmMutateCh:           make(chan interface{}, 128),
		fsmSnapshotCh:         make(chan *reqSnapshotFuture),
		leaderCh:              make(chan bool, 1),
		localID:               conf.LocalID,
		localAddr:             trans.LocalAddr(),
		logger:                logger,
		logStore:              logs,
		configurationChangeCh: make(chan *configurationChangeFuture),
		rpcCh:                 trans.Consumer(),
		snapshotStore:         snaps,
		userSnapshotCh:        make(chan *userSnapshotFuture),
		userRestoreCh:         make(chan *userRestoreFuture),
		shutdownCh:            make(chan struct{}),
		stableStore:           stable,
		transport:             trans,
		verifyCh:              make(chan *verifyFuture, 64),
		configurationsCh:      make(chan *configurationsFuture, 8),
		bootstrapCh:           make(chan *bootstrapFuture),
		observers:             make(map[uint64]*Observer),
	}

	r.conf.Store(*conf)

	// 初始化为 follower.
	r.setState(Follower)

	// restore the current term and the last log.
	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastLog.Index, lastLog.Term)

	// 尝试恢复快照.
	if err = r.restoreSnapshot(); err != nil {
		return nil, err
	}

	// 扫描日志,查找任何配置更改日志条目.
	snapshotIndex, _ := r.getLastSnapshot()
	for i := snapshotIndex + 1; i <= lastLog.Index; i++ {
		var entry Log
		if err = r.logStore.GetLog(i, &entry); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			panic(err)
		}
		if err = r.processConfigurationLogEntry(&entry); err != nil {
			return nil, err
		}
	}
	r.logger.Info("initial configuration", "index", r.configurations.latestIndex,
		"servers", hclog.Fmt("%+v", r.configurations.latest.Servers))

	// 建立心跳处理程序作为一种 fast-path(快传).为了避免磁盘 IO 的队头阻塞.
	// 与阻塞 RPC 同时调用它必须是安全的.
	trans.SetHeartbeatHandler(r.processHeartbeat)

	// start the background work.
	r.goFunc(r.run)
	r.goFunc(r.runFSM)
	r.goFunc(r.runSnapshots)
	return r, nil
}

// restoreSnapshot 尝试恢复最新的快照,如果都无法恢复则失败.该函数在初始化时调用,其他地方调用不安全.
func (r *Raft) restoreSnapshot() error {
	snapshots, err := r.snapshotStore.List()
	if err != nil {
		r.logger.Error("failed to list snapshots,error: ", err)
		return err
	}

	// 尝试加载,按照从新到旧的顺序.
	for _, snapshot := range snapshots {
		if !r.config().NoSnapshotRestoreOnStart {
			var source io.ReadCloser
			_, source, err = r.snapshotStore.Open(snapshot.ID)
			if err != nil {
				r.logger.Error("failed to open snapshot", "id", snapshot.ID, "error", err)
				continue
			}

			if err = fsmRestoreAndMeasure(r.fsm, source); err != nil {
				source.Close()
				r.logger.Error("failed to restore snapshot", "id", snapshot.ID, "error", err)
				continue
			}
			source.Close()
			r.logger.Info("restored from snapshot", "id", snapshot.ID)
		}

		// 更新 lastApplied,这样就不需要重放日志.
		r.setLastApplied(snapshot.Index)

		// 更新最后的持久化快照信息.
		r.setLastSnapshot(snapshot.Index, snapshot.Term)

		// 更新配置.
		var conf Configuration
		var index uint64
		if snapshot.Version >= 0 {
			conf = snapshot.Configuration
			index = snapshot.ConfigurationIndex
		}
		r.setCommittedConfiguration(conf, index)
		r.setLatestConfiguration(conf, index)

		return nil
	}

	// 存在快照,但是加载失败.
	if len(snapshots) > 0 {
		return fmt.Errorf("failed to load any existing snapshots")
	}
	return nil
}

func (r *Raft) config() Config {
	return r.conf.Load().(Config)
}

// BootstrapCluster 等价于非成员 BootstrapCluster,但可以在创建后在未引导的 Raft 实例上调用.
// 这应该只在集群开始时在配置相同的所有 Voter 服务器上调用.
func (r *Raft) BootstrapCluster(configuration Configuration) Future {
	bootstrapReq := &bootstrapFuture{}
	bootstrapReq.init()
	bootstrapReq.configuration = configuration
	select {
	case <-r.shutdownCh:
		bootstrapReq.respond(ErrRaftShutdown)
		return bootstrapReq
	case r.bootstrapCh <- bootstrapReq:
		return bootstrapReq
	}
}

// Leader 返回当前集群中的 leader.
// 如果当前没有 leader 或不知道 leader,返回空.
func (r *Raft) Leader() ServerAddress {
	r.leaderLock.Lock()
	leader := r.leader
	r.leaderLock.Unlock()
	return leader
}

// LastContact 返回最后一次与 leader 联系的 time.Time.
func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

// AddVoter 将给定的服务器作为 Staging 添加到集群中.如果服务器已经是集群中 Voter,则更新服务器的 ServerAddress.
// 当服务器准备就绪,leader 会自动将 Staging 服务器提升为 Voter.
// 'prevIndex' 如果非零,是可以应用此更改的唯一配置的索引;如果同时添加了另一个配置条目,则此请求将失败.
// 'timeout' 如果非零,作为配置条目被追加的超时时间.
func (r *Raft) AddVoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// AddNonvoter 将给定的服务器添加到集群中,但不会分配投票权利.服务器将接收日志条目,但不会参与选举或日志条目提交.
// 如果服务器已经在集群中,这将更新服务器的地址.
func (r *Raft) AddNonvoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:       AddNonvoter,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// RemoveServer 将从集群中删除给定的服务器.如果当前的领导者正在被移除,它将导致发生新的选举.
func (r *Raft) RemoveServer(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// DemoteVoter 将拿走服务器的投票(如果有的话).如果存在,服务器将继续接收日志条目,但不会参与选举或日志条目提交.
// 如果服务器不在集群中,则什么也不做.
func (r *Raft) DemoteVoter(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	return r.requestConfigChange(configurationChangeRequest{
		command:   DemoteVoter,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// Shutdown 用于停止 Raft 后台程序.这不是一个优雅的操作.
// 提供可用于阻塞直到所有后台例程退出的 future.
// TODO 如何优雅关闭?
func (r *Raft) Shutdown() Future {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
		return &shutdownFuture{r}
	}

	// 避免二次关闭 transport.
	return &shutdownFuture{nil}
}

// Snapshot 用于手动强制 Raft 进行快照.返回可用于阻塞直到完成的 future,并且包含可用于打开快照的函数.
func (r *Raft) Snapshot() SnapshotFuture {
	future := &userSnapshotFuture{}
	future.init()
	select {
	case r.userSnapshotCh <- future:
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	}
}

// Restore 是用来手动强制 Raft 使用外部快照的,例如从备份中恢复.我们将使用当前的 Raft 配置,而不是快照中的配置,
// 这样我们就可以恢复到一个新的集群.我们还将使用快照的索引或当前的索引中较高的一个,然后在此基础上加 1,这样我们就
// 可以在 Raft 日志中强制形成一个新的状态,这样快照就会被发送给跟随者并用于任何新的加入者.
// 这只能在领导者身上运行,并阻止其恢复,直到恢复完成或发生错误.
func (r *Raft) Restore(meta *SnapshotMeta, reader io.Reader, timeout time.Duration) error {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	future := &userRestoreFuture{
		meta:   meta,
		reader: reader,
	}
	future.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.userRestoreCh <- future:
		// 如果 future 被使用,则等待它完成.
		if err := future.Error(); err != nil {
			return err
		}
	}

	// 应用一个 no-op 的日志条目.等到 followers 得到恢复并且复制了这个条目,
	// 表明我们已经出现了故障,并且使用 restore 的内容安装了快照.
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	noop.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.applyCh <- noop:
		return noop.Error()
	}
}

// Apply 用于以强一致的方式将命令应用到 FSM. 这将返回一个可用于等待应用程序的 future.
// 'timeout' 来限制我们等待命令执行的时间.这必须在领导者上运行,否则它将失败.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	return r.ApplyLog(Log{Data: cmd}, timeout)
}

// ApplyLog 执行 Apply 但只接受 Log. 从提交的日志中获取的唯一值是 Data 和 Extensions.
func (r *Raft) ApplyLog(log Log, timeout time.Duration) ApplyFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	future := &logFuture{
		log: Log{
			Type:       LogCommand,
			Data:       log.Data,
			Extensions: log.Extensions,
		},
	}
	future.init()

	select {
	case <-timer:
		future.respond(ErrEnqueueTimeout)
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	case r.applyCh <- future:
		return future
	}
}

// Barrier 用于发出一个阻塞命令,直到所有前面的操作都应用于 FSM. 它可用于反映 FSM 所有排队的写入情况.
func (r *Raft) Barrier(timeout time.Duration) Future {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	future := &logFuture{
		log: Log{
			Type: LogBarrier,
		},
	}
	future.init()

	select {
	case <-timer:
		future.respond(ErrEnqueueTimeout)
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	case r.applyCh <- future:
		return future
	}
}

// VerifyLeader 用于确保当前节点仍然还是 leader.
// 可以这样做以防止在可能选出新的 leader 时发生陈旧的读取.
func (r *Raft) VerifyLeader() Future {
	future := &verifyFuture{}
	future.init()
	select {
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	case r.verifyCh <- future:
		return future
	}
}

// GetConfiguration 返回最新的配置,这可能尚未提交.
func (r *Raft) GetConfiguration() ConfigurationFuture {
	future := &configurationsFuture{}
	future.init()
	future.configurations = configurations{latest: r.getLastConfiguration()}
	future.respond(nil)
	return future
}

// State 用于返回当前 raft state.
func (r *Raft) State() RaftState {
	return r.getState()
}

// LeaderCh 用于获取一个 channel,该通道传递有关获得或失去领导权的信号.
// 如果我们成为 leader,它会发送 true.如果我们失去它,它会发送 false.
//
// 仅当 leadership change 时,接受者才会收到通知.
func (r *Raft) LeaderCh() <-chan bool {
	return r.leaderCh
}

// LastIndex 从最后一个日志或最后一个快照返回稳定存储中的最后一个索引.
func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

// AppliedIndex 返回应用于 FSM 的最后一个索引.这通常落后于最后一个索引,
// 特别是对于那些已持久化但尚未被领导者认为已提交的索引.
func (r *Raft) AppliedIndex() uint64 {
	return r.getLastApplied()
}

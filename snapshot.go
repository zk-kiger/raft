package raft

import (
	"fmt"
	"io"
)

// 生成快照流程:
//  1.用户调用 raft.snapshot 方法 -> userSnapshotCh -> runSnapshots() -> takeSnapshots() -> fsmSnapshotCh 根据
//      FSM state 生成的快照来持久化到 SnapshotStore;
//  2.系统触发定时生成快照 -> runSnapshots() -> takeSnapshots() -> fsmSnapshotCh.

// SnapshotMeta 快照元数据.
type SnapshotMeta struct {
	// Version 快照元数据版本,并不包括快照中应用程序数据,应该单独控制.默认为 0.
	Version SnapshotVersion

	ID string

	// Index 和 Term 在生成快照时被存储.
	Index uint64
	Term  uint64

	// Configuration 和 ConfigurationIndex 标识生成快照时的配置及配置索引.
	Configuration      Configuration
	ConfigurationIndex uint64

	// Size 快照的字节个数.
	Size int64
}

// SnapshotStore 接口用于灵活实现快照存储和检索.例如,客户端可以实现共享状态存储,
// 例如 S3,允许新节点在不从领导者流式传输的情况下恢复快照.
type SnapshotStore interface {
	// Create 用于在给定的索引和任期处生成快照,并使用已提交配置.
	// version 表示要创建的快照版本.
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	// List 用于列出存储中可用快照的元数据.它应该按降序返回,先是最高的索引快照.
	List() ([]*SnapshotMeta, error)

	// Open 根据快照 id 获取快照元数据和一个 ReadCloser.一旦调用 Closer 就表示快照不在使用.
	// ReadCloser 用于读取快照应用程序数据.
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// SnapshotSink 由 StartSnapshot 返回.需要 FSM 将 state 写入(调用 Write)
// 到 SnapshotSink 并在完成后调用 Close 关闭 IO.出错时,调用 Cancel.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// runSnapshots 是一个长期运行的 goroutine,用于管理获取 FSM 的新快照.
// 它与 FSM 和主 goroutine 并行运行,因此快照不会阻塞正常操作.
func (r *Raft) runSnapshots() {
	for {
		select {
		case <-randomTimeout(r.config().SnapshotInterval):
			// 检查是否能够生成快照.
			if !r.shouldSnapshot() {
				continue
			}
			// 触发生成快照.
			r.logger.Info("take snapshot")
			if _, err := r.takeSnapshot(); err != nil {
				r.logger.Error("failed to take snapshot", "error", err)
			}

		case future := <-r.userSnapshotCh:
			// 用户主动触发生成快照,立即执行.
			id, err := r.takeSnapshot()
			if err != nil {
				r.logger.Error("failed to take snapshot", "error", err)
			} else {
				future.opener = func() (*SnapshotMeta, io.ReadCloser, error) {
					return r.snapshotStore.Open(id)
				}
			}
			future.respond(err)

		case <-r.shutdownCh:
			return
		}
	}
}

// shouldSnapshot 检查是否满足生成快照的条件.
func (r *Raft) shouldSnapshot() bool {
	lastSnapshot, _ := r.getLastSnapshot()

	lastIdx, err := r.logStore.LastIndex()
	if err != nil {
		r.logger.Error("failed to get last log index", "error", err)
		return false
	}

	gap := lastIdx - lastSnapshot
	return gap >= r.config().SnapshotThreshold
}

// takeSnapshot 用于生成快照,返回新快照的 id 和 error.
func (r *Raft) takeSnapshot() (string, error) {
	// 为 fsm 创建一个执行快照的请求.
	snapReq := &reqSnapshotFuture{}
	snapReq.init()

	// 分发并等待 fsm 生成快照.
	select {
	case r.fsmSnapshotCh <- snapReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}

	// 等待 fsm 响应.
	if err := snapReq.Error(); err != nil {
		if err != ErrNothingNewToSnapshot {
			err = fmt.Errorf("failed to start snapshot: %v", err)
		}
		return "", err
	}
	defer snapReq.snapshot.Release()

	// 请求配置并提取提交信息.
	configReq := &configurationsFuture{}
	configReq.init()
	select {
	case r.configurationsCh <- configReq:
		if err := configReq.Error(); err != nil {
			return "", err
		}
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}
	committed := configReq.configurations.committed
	committedIndex := configReq.configurations.committedIndex

	// 如果存在新的配置正在更新,不能生成快照.
	// 'snapReq.index' 记录 fsm 最后一个日志条目的索引.
	if snapReq.index < committedIndex {
		return "", fmt.Errorf("cannot take snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			committedIndex, snapReq.index)
	}

	// 创建新的快照.
	r.logger.Info("starting snapshot up to", "index", snapReq.index)
	sink, err := r.snapshotStore.Create(SnapshotVersion(r.protocolVersion), snapReq.index, snapReq.term, committed, committedIndex, r.transport)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}

	// 尝试持久化快照.
	if err = snapReq.snapshot.Persist(sink); err != nil {
		sink.Cancel()
		return "", fmt.Errorf("failed to persist snapshot: %v", err)
	}

	// 使用流完成,正常关闭 sink.
	if err = sink.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}

	// 更新最后一个快照信息.
	r.setLastSnapshot(snapReq.index, snapReq.term)

	// 压缩日志.
	if err = r.compactLogs(snapReq.index); err != nil {
		return "", err
	}

	r.logger.Info("snapshot complete up to", "index", snapReq.index)
	return sink.ID(), nil
}

// compactLogs 获取快照包含的最后一个日志索引,并删除不再需要的日志.
func (r *Raft) compactLogs(snapIdx uint64) error {
	// 确定日志压缩的范围.
	minLog, err := r.logStore.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}

	// 检查是否有足够的日志去截断.我们要保证在快照后留下 TrailingLogs 的日志数量.
	lastLogIdx, _ := r.getLastLog()
	trailingLogs := r.config().TrailingLogs
	if lastLogIdx <= trailingLogs {
		return nil
	}

	// 'snapIdx' 截断到快照末尾.
	// 'lastLogIdx-trailingLogs' 从日志末尾向前 'TrailingLogs' 个日志的位置.
	// 保证了至少不允许删除 'TrailingLogs' 个日志.
	//
	// 下面展示两种情况,分别标出每次截取的位置:
	//                       snapIdx<1>
	//               2      1   |
	// -----------------------------------------
	//               |      | <-TrailingLogs-> |
	//           snapIdx<2>                lastLogIdx
	maxLog := min(snapIdx, lastLogIdx-trailingLogs)

	if minLog > maxLog {
		r.logger.Info("no logs to truncate")
		return nil
	}

	r.logger.Info("compacting logs", "from", minLog, "to", maxLog)

	// 压缩日志.
	if err := r.logStore.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}
	return nil
}

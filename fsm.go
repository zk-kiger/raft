package raft

import (
	"fmt"
	"io"
)

// FSM 提供一个可以由客户端实现,用于使用复制日志的接口.
type FSM interface {

	// Apply 一旦提交了日志条目,就会调用应用日志.它返回一个值,如果该方法在
	// 与 FSM 相同的 Raft 节点上调用,则该值将在 Raft.Apply 方法返回的
	// ApplyFuture 中可用.
	Apply(*Log) interface{}

	// Snapshot 用于支持日志压缩.此调用应返回 FSMSnapshot,可用于保存 FSM 某个时间点的快照.
	// Apply 和 Snapshot 不会在多个线程中调用,但 Apply 会与 Persist 并发调用.这需要 FSM
	// 允许在快照生成时进行并发更新 state 的方式来实现.
	Snapshot() (FSMSnapshot, error)

	// Restore 用于从快照(可来自外部)中还原 FSM. 不会任何命令并发调用, FSM 必须丢弃之前的 state.
	Restore(io.ReadCloser) error
}

// FSMSnapshot 由 FSM 返回响应快照.调用 FSMSnapshot 的方法并同时调用 Apply 必须时并发安全的.
type FSMSnapshot interface {
	// Persist 必须将所有必要的 state 持久化到 WriteCloser 'sink',
	// 并在完成时调用 sink.Close() 或错误时调用 sink.Cancel().
	Persist(sink SnapshotSink) error

	// Release 当完成快照时调用释放.
	Release()
}

// runFSM 是一个长期运行的 goroutine,负责将日志应用到 FSM.
// 与其他日志异步执行,因为不希望 FSM 阻止主线程内部操作.
func (r *Raft) runFSM() {
	var lastIndex, lastTerm uint64

	configStore, configStoreEnabled := r.fsm.(ConfigurationStore)

	commitSingle := func(req *commitTuple) {
		// 如果是命令或配置改变的日志,就应用.
		var resp interface{}
		// 确保返回响应.
		defer func() {
			// 如果给了 future,就调用.
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}()

		switch req.log.Type {
		case LogCommand:
			resp = r.fsm.Apply(req.log)

		case LogConfiguration:
			if !configStoreEnabled {
				return
			}
			configStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
		}

		// Update the indexes
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}

	restore := func(req *restoreFuture) {
		// 打开快照.
		meta, source, err := r.snapshotStore.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}
		defer source.Close()

		// 尝试重新存储.
		if err := fsmRestoreAndMeasure(r.fsm, source); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			return
		}

		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	snapshot := func(req *reqSnapshotFuture) {
		// 没有东西可以生成快照.
		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		// start a snapshot.
		snap, err := r.fsm.Snapshot()

		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	for {
		select {
		case ptr := <-r.fsmMutateCh:
			switch req := ptr.(type) {
			case []*commitTuple:
				for _, ct := range req {
					commitSingle(ct)
				}

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh:
			snapshot(req)

		case <-r.shutdownCh:
			return
		}
	}
}

// fsmRestoreAndMeasure 调用 FSM Restore 快照数据.
// 在所有情况下,调用者仍负责在源上调用 Close.
func fsmRestoreAndMeasure(fsm FSM, source io.ReadCloser) error {
	if err := fsm.Restore(source); err != nil {
		return err
	}
	return nil
}

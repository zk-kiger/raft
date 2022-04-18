package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Future 异步响应接口.
type Future interface {

	// Error 阻塞直到 Future 返回错误状态.一个 Future 实例只能
	// 设置一次 Error,Error() 可以被调用任意次 - 得到结果都相同.
	Error() error
}

// IndexFuture 表示一个 raft log entry 被创建的结果.
type IndexFuture interface {
	Future

	// Index holds the newly applied log entry,
	// 在 Error() 返回之前不能调用此方法.
	Index() uint64
}

// ApplyFuture 用于 apply 并且返回 FSM 的 Response.
type ApplyFuture interface {
	IndexFuture

	// Response 当 FSM.Apply 方法返回时返回 FSM Response.
	// note: 如果 FSM.Apply 返回一个 error,会通过 Response 返回,
	// 而不是 Error,即 FSM 检查 Response 的 errors 更为重要.
	Response() interface{}
}

// ConfigurationFuture 用于 Configuration,可以返回 Raft 使用的最新配置.
type ConfigurationFuture interface {
	IndexFuture

	// Configuration 包含最新的配置.在 Error 方法返回之前不能调用它.
	Configuration() Configuration
}

// SnapshotFuture 用于等待用户触发的快照生成完成.
type SnapshotFuture interface {
	Future

	// Open 是一个函数,可以调用它来访问底层快照及其元数据.在 Error 方法返回之前不能调用它.
	Open() (*SnapshotMeta, io.ReadCloser, error)
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

// deferError 可以嵌入地允许 future 提供一个 error.
type deferError struct {
	err error

	// errCh 确保在设置 error 不会出现并发问题,
	// 保证 Error 语义正确性(每次调用得到的结果相同).
	errCh chan error

	// responded 只允许提供一个 error,初始为 false,
	// 在调用 Error() 之后为 true.
	responded  bool
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// note: 在接收到一个 nil 的 error,不会触发,
		// 但是 errCh 通过会关闭,所以还是会返回 nil error.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// configurationChangeFuture 有几种类型的请求会导致将配置条目附加到日志中.
// 这些在此处被编码以供 leaderLoop() 处理.
type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

// bootstrapFuture 用于尝试 cluster 的实时引导.
type bootstrapFuture struct {
	deferError

	// configuration 建议应用的 bootstrap configuration.
	configuration Configuration
}

// logFuture 用于 apply a log entry 并等待,直到被大多数节点提交.
type logFuture struct {
	deferError

	log      Log
	response interface{}
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.transport.(WithClose); ok {
		closeable.Close()
	}
	return nil
}

// userSnapshotFuture 用于等待用户触发的快照生成完成.
type userSnapshotFuture struct {
	deferError

	// opener 用于打开快照的函数.如果在生成快照之后没有错误,该函数就会被填写.
	opener func() (*SnapshotMeta, io.ReadCloser, error)
}

// Open 实现 SnapshotFuture 接口.
func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	}
	// 令 opener 失效,因此该函数不能被多次调用.
	defer func() {
		u.opener = nil
	}()
	return u.opener()
}

// userRestoreFuture 用于等待用户触发的外部快照恢复完成.
type userRestoreFuture struct {
	deferError

	// meta 是属于快照的元数据.
	meta *SnapshotMeta

	// reader 读取快照内容的接口.
	reader io.Reader
}

// reqSnapshotFuture 用于请求快照生成,只用于内部使用(fsm).
type reqSnapshotFuture struct {
	deferError

	// FSM 在响应之前提供的快照详细信息.
	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	deferError
	ID string
}

// verifyFuture is used to verify the current node is still
// the leader. This is to prevent a stale read.
type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	voteLock   sync.Mutex
}

// vote 用于回应 verifyFuture.
// 可能会在回应 notifyCh 上阻塞.
func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	// 防止已经通知.
	if v.notifyCh == nil {
		return
	}

	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

// configurationsFuture 用于获取当前 configurations 信息.
type configurationsFuture struct {
	deferError
	configurations configurations
}

// Configuration 返回 Raft 最新的配置.
func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

// Index 返回 Raft 最新配置的索引.
func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

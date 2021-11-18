package raft

import "time"

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
		d.err = ErrorRaftShutdown
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


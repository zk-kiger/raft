package raft

import (
	"github.com/hashicorp/go-hclog"
	"io"
	"time"
)

type ProtocolVersion int

type SnapshotVersion int

type Config struct {
	// ProtocolVersion 协议版本号,默认为 0.
	ProtocolVersion ProtocolVersion

	// HeartbeatTimeout 指定在我们尝试选举之前没有领导者的跟随者状态的时间.
	HeartbeatTimeout time.Duration

	// ElectionTimeout 指定在我们尝试选举之前没有领导者的候选状态的时间.
	ElectionTimeout time.Duration

	// CommitTimeout 在心跳之前控制没有执行 Apply 操作的时间,确保可以及时提交.
	// 由于随机交错,可能会延迟2倍于该值.
	CommitTimeout time.Duration

	// MaxAppendEntries 控制一次发送的最大追加条目数.如果追随者因为日志不一致而拒绝,
	// 希望在效率和避免浪费之间取得平衡.
	MaxAppendEntries int

	// BatchApplyCh 指示是否应该将 applyCh 缓冲到 MaxAppendEntries 的大小.启用批量
	// 日志提交,会破坏 Apply 的超时保证.具体的做法是,将日志添加到 applyCh 缓冲区,但会在
	// 指定的超时时间之后才会实际处理.
	BatchApplyCh bool

	// TrailingLogs 控制在快照后留下多少日志.使用它是为了可以快速重放跟随者的日志,而不是
	// 被迫发送整个日志快照.
	TrailingLogs uint64

	// SnapshotInterval 控制检查是否符合快照的频率.在这个值和这个值的2倍之间随机交错,
	// 以避免整个集群一次执行快照.
	SnapshotInterval time.Duration

	// SnapshotThreshold 控制在执行快照之前生成日志的阈值.通过重放一小部分日志来防止过度快照.
	SnapshotThreshold uint64

	// LeaderLeaseTimeout 用于控制在无法联系法定人数的情况下成为领导者的"租期"持续时间.
	// 如果在没有联系的情况下达到这个间隔,辞去领导者职务.
	LeaderLeaseTimeout time.Duration

	// LocalID 当前服务器任意时刻唯一的 id.
	LocalID ServerID

	// NotifyCh 用于提供将通知领导关系变化的通道.Raft 在写入管道时会阻塞,因此它应该被缓冲或者积极消费.
	NotifyCh chan<- bool

	// LogOutput 用作日志的接收器,除非指定了 Logger.默认是 os.Stderr.
	LogOutput io.Writer

	// LogLevel 日志级别.
	LogLevel string

	// Logger 用户提供的日志记录器.如果为 nil,则使用 LogOutput 作为日志接收器,LogLevel 作为日志级别
	// 创建一个新的 Logger.
	Logger hclog.Logger
}

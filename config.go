package raft

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"time"
)

type ProtocolVersion int

const (
	// ProtocolVersionMin is the minimum protocol version
	ProtocolVersionMin ProtocolVersion = 0
	// ProtocolVersionMax is the maximum protocol version
	ProtocolVersionMax = 3
)

type SnapshotVersion int

const (
	// SnapshotVersionMin is the minimum snapshot version
	SnapshotVersionMin SnapshotVersion = 0
	// SnapshotVersionMax is the maximum snapshot version
	SnapshotVersionMax = 3
)

// Config 提供 raft 服务器一些需要的配置.
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

	// 如果我们是集群中的成员,当被调用 RemoveServer 移除当前节点时,节点会删除与其他 peers
	// 的联系并且转移为 Follower.
	// 如果设置了 ShutdownOnRemove,我们会额外关闭 Raft.
	// 否则,我们可以成为仅包含该节点的集群的 Leader.
	ShutdownOnRemove bool

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
	//NotifyCh chan<- bool

	// LogOutput 用作日志的接收器,除非指定了 Logger.默认是 os.Stderr.
	LogOutput io.Writer

	// LogLevel 日志级别.
	LogLevel string

	// Logger 用户提供的日志记录器.如果为 nil,则使用 LogOutput 作为日志接收器,LogLevel 作为日志级别
	// 创建一个新的 Logger.
	Logger hclog.Logger

	// NoSnapshotRestoreOnStart 控制 raft 是否会在启动时将快照恢复到 FSM.
	// 如果您的 FSM 从 raft 快照以外的其他机制恢复,这将很有用.
	// 快照元数据仍将用于初始化 raft 的配置和索引值.
	NoSnapshotRestoreOnStart bool
}

// DefaultConfig returns a Config with usable defaults.
func DefaultConfig() *Config {
	return &Config{
		ProtocolVersion:    ProtocolVersionMax,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		LogLevel:           "DEBUG",
	}
}

func ValidateConfig(config *Config) error {
	// We don't actually support running as 0 in the library any more, but
	// we do understand it.
	protocolMin := ProtocolVersionMin
	if protocolMin == 0 {
		protocolMin = 1
	}
	if config.ProtocolVersion < protocolMin ||
		config.ProtocolVersion > ProtocolVersionMax {
		return fmt.Errorf("ProtocolVersion %d must be >= %d and <= %d",
			config.ProtocolVersion, protocolMin, ProtocolVersionMax)
	}
	if len(config.LocalID) == 0 {
		return fmt.Errorf("LocalID cannot be empty")
	}
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("HeartbeatTimeout is too low")
	}
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("ElectionTimeout is too low")
	}
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("CommitTimeout is too low")
	}
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries is too large")
	}
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("SnapshotInterval is too low")
	}
	if config.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if config.LeaderLeaseTimeout > config.HeartbeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout cannot be larger than heartbeat timeout")
	}
	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout must be equal or greater than Heartbeat Timeout")
	}
	return nil
}

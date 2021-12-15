package raft

import "io"

// SnapshotMeta 快照元数据.
type SnapshotMeta struct {
	// Version 快照元数据版本,并不包括快照中应用程序数据,应该单独控制.默认为 0.
	Version SnapshotVersion

	ID string

	// Index 和 Term 在生成快照时被存储.
	Index uint64
	Term  uint64

	// Configuration 和 ConfigurationIndex 标识生成快照时的配置及配置索引.
	Configuration Configuration
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
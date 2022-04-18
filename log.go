package raft

import (
	"errors"
	"time"
)

type LogType uint8

const (
	// LogCommand 被应用于 FSM.
	LogCommand LogType = iota

	// LogNoop 被用于维护领导.
	LogNoop

	LogBarrier

	// LogConfiguration 建立成员变更配置.
	// It is created when a server is added, removed, promoted, etc.
	LogConfiguration
)

var (
	// ErrKeyNotFound 表示给定的 key 不存在
	ErrKeyNotFound = errors.New("not found")
)

// Log entry 被复制到 Raft cluster 所有成员,成为复制状态机的核心.
type Log struct {
	// Index holds the index of log entry.
	Index uint64

	// Term holds the election term of log entry.
	Term uint64

	// Type holds the type of log entry.
	Type LogType

	// Data holds the log entry's type-specific(特定类型) data.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware.
	// 保存中间件中不透明的字节信息片段.
	Extensions []byte

	// AppendedAt stores the time the leader first appended this log to it's LogStore.
	// 存储领导者第一次将日志存储到 LogStore 的时间.
	AppendedAt time.Time
}

// LogStore 用于提供以持久化的方式存储和检索日志的接口.
type LogStore interface {
	// FirstIndex 返回第一个写入的索引. 0 表示没有条目.
	FirstIndex() (uint64, error)

	// LastIndex 返回最后一个写入的索引. 0 表示没有条目.
	LastIndex() (uint64, error)

	// GetLog 获取给定索引的日志条目.
	GetLog(index uint64, log *Log) error

	// StoreLog 存储日志条目.
	StoreLog(log *Log) error

	// StoreLogs 批量存储日志条目.
	StoreLogs(logs []*Log) error

	// DeleteRange 删除一系列日志条目.范围包括在 [min, max] 内.
	DeleteRange(min, max uint64) error
}

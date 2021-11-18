package raft

type LogType uint8

const (
	// LogCommand 被应用于 FSM.
	LogCommand LogType = iota

	// LogNoop 被用于维护领导.
	LogNoop
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
}

type LogStore interface {


}

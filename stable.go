package raft

// StableStore 用于提供关键配置的持久化存储,确保安全.
type StableStore interface {
	// Set 设置 key/val.
	Set(key, val []byte) error

	// Get 返回 key 对应的值,如果 key 不存在则返回空切片.
	Get(key []byte) ([]byte, error)

	// SetUint64 设置 key/val,val 为 uint64.
	SetUint64(key []byte, val uint64) error

	// GetUint64 返回 key 对应的 uint64 值,如果 key 不存在则返回 0.
	GetUint64(key []byte) (uint64, error)
}

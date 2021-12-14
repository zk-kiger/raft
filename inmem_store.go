package raft

import "sync"

// InmemStore 实现了 LogStore 和 StableStore 接口.它不能使用于生产,
// 仅使用于单元测试.生产环境改用 BoltDB.
type InmemStore struct {
	lock      sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*Log
	kv        map[string][]byte
	kvInt     map[string]uint64
}

// NewInmemStore 返回一个 in-memory 存储后端.仅用于测试.
func NewInmemStore() *InmemStore {
	i := &InmemStore{
		logs:  make(map[uint64]*Log),
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
	}
	return i
}

// FirstIndex 返回 Raft 日志中第一个已知的索引.
func (i *InmemStore) FirstIndex() (uint64, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.lowIndex, nil
}

// LastIndex 返回 Raft 日志中最后一个已知的索引.
func (i *InmemStore) LastIndex() (uint64, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.highIndex, nil
}

// GetLog 从 BoltDB 中使用给定的 index 检索日志.
func (i *InmemStore) GetLog(index uint64, log *Log) error {
	i.lock.RLock()
	defer i.lock.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// StoreLog 用于存储单个 Raft 日志.
func (i *InmemStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

// StoreLogs 用于存储 Raft 日志集合.
func (i *InmemStore) StoreLogs(logs []*Log) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

// DeleteRange 删除给定区间内的 Raft 日志.
func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}

	// ---- |min ---- [low] ---- max| ---- [high] ----
	if min <= i.lowIndex {
		i.lowIndex = max+1
	}
	// ---- [low] ---- |min ---- [high] ---- max| ----
	if max >= i.highIndex {
		i.highIndex = min-1
	}
	// ---- |min ---- [low] ---- [high] ---- max| ----
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}
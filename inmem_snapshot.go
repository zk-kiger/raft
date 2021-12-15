package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

// InmemSnapshotStore 实现 SnapshotStore 接口,只保留最近的快照.
type InmemSnapshotStore struct {
	lock        sync.RWMutex
	latest      *InmemSnapshotSink
	hasSnapshot bool
}

// InmemSnapshotSink 基于内存实现 SnapshotSink 接口.
type InmemSnapshotSink struct {
	meta     SnapshotMeta
	contents *bytes.Buffer
}

// NewInmemSnapshotStore 创建 InmemSnapshotStore 对象.
func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: &InmemSnapshotSink{
			contents: &bytes.Buffer{},
		},
	}
}

// Create 使用给定参数用新的快照替换存储的快照.
func (i *InmemSnapshotStore) Create(version SnapshotVersion, index, term uint64, configuration Configuration,
	configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	name := snapshotName(term, index)
	i.lock.Lock()
	defer i.lock.Unlock()

	sink := &InmemSnapshotSink{
		meta: SnapshotMeta{
			Version: version,
			ID: name,
			Index: index,
			Term: term,
			Configuration: configuration,
			ConfigurationIndex: configurationIndex,
		},
		contents: &bytes.Buffer{},
	}
	i.hasSnapshot = true
	i.latest = sink

	return sink, nil
}

// List 返回最近的快照,否则返回空集合.
func (i *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if !i.hasSnapshot {
		return []*SnapshotMeta{}, nil
	}
	return []*SnapshotMeta{&i.latest.meta}, nil
}

// Open 将快照应用程序数据包装为 io.ReadCloser.
func (i *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if i.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot id: %s", id)
	}

	// 生成副本,因为 contents 只读.
	contents := bytes.NewBuffer(i.latest.contents.Bytes())
	return &i.latest.meta, ioutil.NopCloser(contents), nil
}

// Write 追加给定字节到快照应用程序数据.
func (s *InmemSnapshotSink) Write(p []byte) (n int, err error) {
	written, err := s.contents.Write(p)
	s.meta.Size += int64(written)
	return written, err
}

// Close 更新快照大小,否则 no-op 操作.
func (s *InmemSnapshotSink) Close() error {
	return nil
}

// ID 返回 SnapshotMeta ID.
func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

// Cancel 返回 nil.
func (s *InmemSnapshotSink) Cancel() error {
	return nil
}

// snapshotName 为快照生成 ID.
func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}
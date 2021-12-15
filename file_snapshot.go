package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	testPath      = "permTest"
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"
)

// FileSnapshotStore 实现 SnapshotStore 接口,允许快照在本地磁盘保存.
type FileSnapshotStore struct {
	path   string
	retain int
	logger hclog.Logger

	// noSync 如果为 true,则跳过 crash-safe file fsync 同步方法调用.
	// 私有字段,仅用于测试.默认为 false,在测试是可以开启.
	noSync bool
}

type snapMetaSlice []*fileSnapshotMeta

// fileSnapshotMeta 存储在磁盘.并且放入 CRC 用于校验快照.
type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

// FileSnapshotSink 实现 SnapshotSink 接口.
type FileSnapshotSink struct {
	store     *FileSnapshotStore
	logger    hclog.Logger
	dir       string
	parentDir string
	meta      fileSnapshotMeta

	noSync bool

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

type bufferedFile struct {
	bfl *bufio.Reader
	fl  *os.File
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	return b.bfl.Read(p)
}

func (b *bufferedFile) Close() error {
	return b.fl.Close()
}

// NewFileSnapshotStoreWithLogger 基于基础目录创建一个新的 FileSnapshotStore,
// `retain` 参数控制保留多少快照,至少是 1.
func NewFileSnapshotStoreWithLogger(base string, retain int, logger hclog.Logger) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "snapshot",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	// 指定目录下创建新的目录,确保路径存在.
	// /base ==> /base/snapshots
	path := filepath.Join(base, snapPath)
	// perm 用于设置文件权限[-rwxr-xr-x],os.IsExist 用于判断错误(文件是否已经存在).
	if err := os.MkdirAll(path, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	// 文件权限测试.
	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}

	return store, nil
}

// NewFileSnapshotStore 基于基础目录创建一个新的 FileSnapshotStore,
// `retain` 参数控制保留多少快照,至少是 1.
func NewFileSnapshotStore(base string, retain int, logOutput io.Writer) (*FileSnapshotStore, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewFileSnapshotStoreWithLogger(base, retain, hclog.New(&hclog.LoggerOptions{
		Name:   "snapshot",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	}))
}

// testPermissions 尝试在路径目录下创建文件看是否生效.
func (f *FileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}

	if err = fh.Close(); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

// Create 用于创建一个新的快照.
func (f *FileSnapshotStore) Create(version SnapshotVersion, index, term uint64, configuration Configuration,
	configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	// 创建新的路径.
	name := snapshotName(term, index)
	// /base/snapshots ==> /base/snapshots/1-1-1.tmp
	// 临时文件,用于用户写入 state.在关闭文件时,去掉 tmpSuffix.
	path := filepath.Join(f.path, name+tmpSuffix)
	f.logger.Info("creating new snapshot, path: ", path)

	// 生成目录.
	if err := os.MkdirAll(path, 0755); err != nil && !os.IsExist(err) {
		f.logger.Error("filed to make snapshot directly, error: ", err)
		return nil, err
	}

	// 创建 Sink.
	sink := &FileSnapshotSink{
		store:     f,
		logger:    f.logger,
		dir:       path,
		parentDir: f.path,
		noSync:    f.noSync,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	// 将元数据写入文件.
	if err := sink.writeMeta(); err != nil {
		f.logger.Error("failed to write metadata, error: ", err)
		return nil, err
	}

	// 打开 state 文件.
	// /base/snapshots/1-1-1.tmp ==> /base/snapshots/1-1-1.tmp/state.bin
	statePath := filepath.Join(f.path, stateFilePath)
	fl, err := os.Create(statePath)
	if err != nil {
		f.logger.Error("failed to create state file, error: ", err)
		return nil, err
	}

	sink.stateFile = fl

	// 创建一个 CRC64 Hash.
	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))
	// Wrap both the hash and file in a MultiWriter with buffering
	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	return sink, nil
}

// writeMeta 用于将已有的快照元数据写入文件.
func (s *FileSnapshotSink) writeMeta() error {
	// 打开 meta 文件.
	// /base/snapshots/1-1-1.tmp ==> /base/snapshots/1-1-1.tmp/meta.json
	metaPath := filepath.Join(s.dir, metaFilePath)
	fl, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer fl.Close()

	// buffer the file IO.
	bufferedFl := bufio.NewWriter(fl)

	// 转为 JSON,写入文件.
	encoder := json.NewEncoder(bufferedFl)
	if err = encoder.Encode(&s.meta); err != nil {
		return err
	}

	if err = bufferedFl.Flush(); err != nil {
		return err
	}

	// 将元数据从内存持久化到磁盘文件.
	if !s.noSync {
		if err = fl.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// List 返回存储中可用的快照.
func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots, error: ", err)
		return nil, err
	}

	var snapMeta []*SnapshotMeta
	for _, snapshot := range snapshots {
		snapMeta = append(snapMeta, &snapshot.SnapshotMeta)
		if len(snapMeta) == f.retain {
			break
		}
	}
	return snapMeta, nil
}

// getSnapshots 返回所有已知的快照.
func (f *FileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {
	// 获取符合条件的快照.
	snapshots, err := ioutil.ReadDir(f.path)
	if err != nil {
		f.logger.Error("failed to scan snapshot directory, error: ", err)
		return nil, err
	}

	// 过滤快照数据,填充快照元数据.
	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {
		// 忽略文件.
		if !snap.IsDir() {
			continue
		}

		// 忽略临时文件.
		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Warn("found temporary snapshot, name: ", dirName)
			continue
		}

		// 尝试读取元数据.
		var meta *fileSnapshotMeta
		meta, err = f.readMeta(dirName)
		if err != nil {
			f.logger.Warn("failed to read metadata, name: ", dirName, "error: ", err)
			continue
		}

		// 追加,但是只返回 retain 限制的快照个数.
		snapMeta = append(snapMeta, meta)
	}

	// 逆置且排序, new -> old.
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))
	return snapMeta, nil
}

// readMeta 使用给定的 name 读取元数据.
func (f *FileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {
	// 打开 meta 文件.
	metaPath := filepath.Join(f.path, name, metaFilePath)
	fl, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fl.Close()

	// buffered the file IO.
	buffered := bufio.NewReader(fl)

	// 读取 JSON.
	meta := &fileSnapshotMeta{}
	decoder := json.NewDecoder(buffered)
	if err = decoder.Decode(meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// Open 根据快照 id 获取快照元数据并返回该快照的 ReadCloser.
func (f *FileSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	// 获取元数据.
	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Error("failed to get meta data to open snapshot, error: ", err)
		return nil, nil, err
	}

	// 打开 state 文件.
	statePath := filepath.Join(f.path, id, stateFilePath)
	fl, err := os.Open(statePath)
	if err != nil {
		f.logger.Error("failed to open the state file, error: ", err)
		return nil, nil, err
	}
	defer fl.Close()

	// 创建 crc64 Hash.
	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	// 将文件内容进行 hash.
	_, err = io.Copy(stateHash, fl)
	if err != nil {
		f.logger.Error("failed to read state file, error: ", err)
		return nil, nil, err
	}

	// 校验 CRC.
	computed := stateHash.Sum(nil)
	if bytes.Compare(meta.CRC, computed) != 0 {
		f.logger.Error("CRC checksum failed, store: ", meta.CRC, "computed: ", computed)
		return nil, nil, err
	}

	// 寻找文件指针.Seek 将下一次 Read 或 Write on file 的偏移量设置为 offset,
	// whence：0 表示相对于文件的原点,1 表示相对于当前偏移量,2 表示相对于结尾.
	if _, err = fl.Seek(0, 0); err != nil {
		f.logger.Error("state file seek failed, error: ", err)
		return nil, nil, err
	}

	buffered := &bufferedFile{
		bfl: bufio.NewReader(fl),
		fl:  fl,
	}

	return &meta.SnapshotMeta, buffered, nil
}

// ID 返回 SnapshotMeta ID.
func (s *FileSnapshotSink) ID() string {
	return s.meta.ID
}

// Write 追加字节到 state 文件.使用缓冲 io,为了减少上下文切换的次数.
func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

// Close 表示成功结束.
func (s *FileSnapshotSink) Close() error {
	// 确保 Close 幂等.
	if s.closed {
		return nil
	}
	s.closed = true

	// 关闭文件资源.
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot, error: ", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.logger.Error("failed to delete temporary snapshot directory, path: ", s.dir, "error: ", delErr)
			return delErr
		}
	}

	// 写入更新后的元数据.
	if err := s.writeMeta(); err != nil {
		s.logger.Error("failed to write metadata, error: ", err)
		return err
	}

	// 将快照移动到目录,不再使用临时目录名.
	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.logger.Error("failed to move snapshot into place, error: ", err)
		return err
	}

	// windows 跳过 sync 对目录条目的编辑,仅 *nix 系统需要.
	if !s.noSync && runtime.GOOS != "windows" {
		parentFl, err := os.Open(s.parentDir)
		defer parentFl.Close()
		if err != nil {
			s.logger.Error("failed to open snapshot parent directory, path: ", s.parentDir, "error: ", err)
			return err
		}

		if err = parentFl.Sync(); err != nil {
			s.logger.Error("failed syncing parent directory, path: ", s.parentDir, "error: ", err)
			return err
		}
	}

	// 移除超出保留计数的任何快照.
	if err := s.store.RemoveOverRetainSnapshot(); err != nil {
		return err
	}

	return nil
}

// Cancel 表示未能成功结束.
func (s *FileSnapshotSink) Cancel() error {
	// 确保 Close 幂等.
	if s.closed {
		return nil
	}
	s.closed = true

	// 关闭文件资源.
	if err := s.finalize(); err != nil {
		s.logger.Error("failed to finalize snapshot, error: ", err)
		return err

	}

	// 尝试删除所有组件.
	return os.RemoveAll(s.dir)
}

// finalize 用于关闭所有资源.
func (s *FileSnapshotSink) finalize() error {
	// Flush 缓冲区存留的数据.
	if err := s.buffered.Flush(); err != nil {
		return err
	}

	// 强制 Sync 将内存数据持久化到磁盘.
	if !s.noSync {
		if err := s.stateFile.Sync(); err != nil {
			return err
		}
	}

	// 获取 state FileInfo.
	stat, statErr := s.stateFile.Stat()

	// 关闭 state 文件.
	if err := s.stateFile.Close(); err != nil {
		return nil
	}

	// 获取文件大小,并在关闭文件之后检查.
	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	// 设置 hash.
	s.meta.CRC = s.stateHash.Sum(nil)

	return nil
}

// RemoveOverRetainSnapshot 移除超出保留计数的任何快照.
func (f *FileSnapshotStore) RemoveOverRetainSnapshot() error {
	var err error
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Error("failed to get snapshots, error: ", err)
		return err
	}

	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		f.logger.Info("removing snapshot, path: ", path)
		if err = os.RemoveAll(path); err != nil {
			f.logger.Error("failed to remove snapshot, path: ", path, "error: ", err)
			return err
		}
	}

	return nil
}

// ----- snapMetaSlice []*fileSnapshotMeta 实现 sort 接口. -----

func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

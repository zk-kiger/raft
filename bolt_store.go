package raft

import "github.com/boltdb/bolt"

const (
	// 在 db 文件上使用的权限.仅在数据库文件不存在且需要创建时使用.
	dbFileMode = 0600
)

var (
	// 存储数据的 bucket name.
	dbLogs = []byte("logs")
	dbConf = []byte("conf")
)

// BoltStore 为 Raft 提供对 BoltDB 的访问,以存储和检索日志条目.
// 它还提供了 key/value 存储,可用于 LogStore 和 StableStore.
type BoltStore struct {
	// conn BoltDB 的 handler.
	conn *bolt.DB

	// path 是要使用的 BoltDB 的文件路径.
	path string
}

// Options 包含了用于打开 BoltDB 的所有配置.
type Options struct {
	// Path 是要使用的 BoltDB 的文件路径.
	Path string

	// BoltOptions 包含 BoltDB 一些特定的选项.[e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync 导致数据库在每次写入日志后跳过 fsync 调用.不安全需谨慎设置.
	NoSync bool
}

// readOnly 如果包含的 bolt 选项说以只读模式打开数据库,则 readOnly 返回
// true [这对于想要检查日志的工具很有用].
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// NewBoltStore 获取文件路径并返回连接的 Raft 后端.
func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

// New 使用提供的 options 打开 BoltDB 并准备将其用作 raft 后端.
func New(options Options) (*BoltStore, error) {
	// 尝试连接 db.
	bolt, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	bolt.NoSync = options.NoSync

	// 创建新的 BoltStore.
	store := &BoltStore{
		conn: bolt,
		path: options.Path,
	}

	// 如果 DB 被设置为 ReadOnly,则不允许创建 bucket.
	if !options.readOnly() {

	}
	return store, nil
}

// initialize 用于建立所有 bucket.
func (b *BoltStore) initialize() error {
	// 开启事务.
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	// 如果没有提交就回滚所有更新.
	defer tx.Rollback()

	// 创建存储数据的所有 bucket.
	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	return tx.Commit()
}

// Close 用于优雅的关闭数据库连接.
func (b *BoltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex 返回 Raft 日志中第一个已知的索引.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex 返回 Raft 日志中最后一个已知的索引.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

// GetLog 从 BoltDB 中使用给定的 index 检索日志.
func (b *BoltStore) GetLog(idx uint64, log *Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return ErrLogNotFound
	}
	return decodeMsgPack(val, log)
}

// StoreLog 用于存储单个 Raft 日志.
func (b *BoltStore) StoreLog(log *Log) error {
	return b.StoreLogs([]*Log{log})
}

// StoreLogs 用于存储 Raft 日志集合.
func (b *BoltStore) StoreLogs(logs []*Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange 删除给定区间内的 Raft 日志.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// 处理超出范围的日志索引.
		if bytesToUint64(k) > max {
			break
		}

		// 删除范围内的日志索引.
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}
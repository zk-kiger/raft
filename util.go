package raft

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

func init() {
	// 确保我们为伪随机生成器使用高熵种子.
	rand.Seed(newSeed())
}

// newSeed 从加密随机源返回 int64 为 math/rand 提供源.
// 使用 crand 包加密随机获取 Int ,来实现更加安全的随机生成器.
func newSeed() int64 {
	// Int returns a uniform random value in [0, max). It panics if max <= 0.
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout 返回一个在 [minVal, 2*minVal] 之间的数.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := time.Duration(rand.Int63()) % minVal
	return time.After(minVal + extra)
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID 用于生成随机的 UUID.
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotifyCh 异步地通知 ch.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// overrideNotifyBool 用于通知 bool channel,但如果存在值,则覆盖现有值.
// ch 必须是缓冲大小为 1 的 channel.
// 这个方法不支持并发调用.
func overrideNotifyBool(ch chan bool, v bool) {
	select {
	case ch <- v:
	case <-ch:
		// channel had an old value.
		select {
		case ch <- v:
		default:
			panic("race: channel was sent concurrently")
		}
	}
}

// backoff 是根据失败次数来控制退避持续时间,直到达到最大退避时间.
func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

// 需要排序 []uint64 使用,用于 commitment.
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

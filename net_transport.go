package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")
)

/*

NetworkTransport 提供了一种基于网络的传输,可用于与远程机器上的 Raft 通信.
它需要一个底层的流来提供流抽象,可以是简单的 TCP、UDP、TLS 等.

这种运输方式非常简单和轻便.
每个 RPC 请求都是通过发送一个指示消息类型的字节,然后是 MsgPack 编码的请求来构建的.

响应是一个错误字符串后跟响应对象,两者都使用 MsgPack 编码.

InstallSnapshot 很特别,因为在 RPC 请求之后我们处于传输快照状态.该套接字
不会重新使用(不会返回给连接池),因为如果出现错误,连接状态是未知的.

*/
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan RPC

	heartbeatFunc     func(RPC)
	heartbeatFuncLock sync.Mutex

	logger hclog.Logger

	maxPool int

	serverAddressProvider ServerAddressProvider

	stream StreamLayer
	// streamCtx 用于退出正在存在的 conn handlers.
	streamCtx        context.Context
	streamCancelFunc context.CancelFunc
	streamCtxLock    sync.RWMutex

	timeout time.Duration
	// TimeoutScale 用于调和超时时间,
	// 默认超时时间根据 DataSize/TimeoutScale(256KB) s,TimeoutScale = 256 * 1024.
	TimeoutScale int

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NetworkTransportConfig 封装网络传输层配置.
type NetworkTransportConfig struct {
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool 控制连接池的数量.
	MaxPool int

	// Timeout 用于 I/O deadline 超时.
	Timeout time.Duration
}

// ServerAddressProvider 用于在建立连接调用 RPC 时,覆盖目标地址,类似于服务注册中心.
// (ServerID 可能对应多个 ServerAddress,可用于负载均衡等操作).
type ServerAddressProvider interface {
	ServerAddress(id ServerID) (ServerAddress, error)
}

// StreamLayer 提供流传输层的接口抽象,可接入 TCP、UDP、TSL 等底层传输协议.
type StreamLayer interface {
	net.Listener

	// Dial 用于创建新的对外连接.
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

// netConn 封装监听到的目标连接,通过包装方便发送与响应 RPC 请求.
type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

// Release 释放连接.
func (n *netConn) Release() error {
	return n.conn.Close()
}

// NewNetworkTransportWithConfig 使用给定的配置创建新的网络传输对象.
func NewNetworkTransportWithConfig(config *NetworkTransportConfig) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}

	transport := &NetworkTransport{
		connPool:              make(map[ServerAddress][]*netConn),
		consumeCh:             make(chan RPC),
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          DefaultTimeoutScale,
		serverAddressProvider: config.ServerAddressProvider,
	}

	// create stream connection context,then start to listen conn.
	transport.createStreamContext()
	go transport.listen()

	return transport
}

// NewNetworkTransport 使用给定的配置创建新的网络传输对象,
// maxPool 控制连接池中 conn 的个数,
// timeout 用于 I/O deadline 超时时间.
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger 使用给定的配置创建新的网络传输对象,
// maxPool 控制连接池中 conn 的个数,
// timeout 用于 I/O deadline 超时时间.
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// createStreamContext 用于创建 stream context.应该在持有 streamLock 的情况下调用.
func (n *NetworkTransport) createStreamContext() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancelFunc = cancelFunc
}

// getStreamContext 获取 stream context.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// CloseStreams 关闭当前的网络流.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// 释放 connPool 连接池中的 conn,并删除.
	for addr, conns := range n.connPool {
		for _, conn := range conns {
			conn.Release()
		}
		delete(n.connPool, addr)
	}

	// cancel 已经存在的 conn,并重新创建 streamCtx.这两个操作必须都在持有
	// streamCtxLock 的情况下,否则可能会出现 streamCtx 无法被 cancel 的并发问题.
	n.streamCtxLock.Lock()
	n.streamCancelFunc()
	n.createStreamContext()
	n.streamCtxLock.Unlock()
}

// Close 用于停止 network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}

	return nil
}

// IsShutdown 用于判断 Network transport 是否被关闭.
// 当调用 Close 方法时,会关闭 shutdownCh,那么该方法会返回 true.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// Consumer 实现 Transport 接口.
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr 实现 Transport 接口.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// AppendEntries 实现 Transport 接口.
func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote 实现 Transport 接口.
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// genericRPC 处理简单的 request/response RPC.
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	// get a conn.
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// set conn's deadline.
	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// send the RPC.
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// decode the response.
	connCanReturn, err := decodeResponse(conn, resp)
	if connCanReturn {
		n.returnConn(conn)
	}
	return err
}

// getConnFromAddressProvider 返回来自 ServerAddressProvider 的连接(如果可用),或者默认使用目标服务器地址的连接.
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddress(id)
		if err != nil {
			n.logger.Warn("unable to get address for server, using fallback address, id: ", id, "fallback: ", target, "error: ", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn 用于从 connPool(连接池)获取 conn.
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	// 从 connPool 中获取.
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new conn.
	dial, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	netConn_ := &netConn{
		target: target,
		conn:   dial,
		w:      bufio.NewWriter(dial),
		dec:    codec.NewDecoder(bufio.NewReader(dial), &codec.MsgpackHandle{}),
	}
	netConn_.enc = codec.NewEncoder(netConn_.w, &codec.MsgpackHandle{})

	return netConn_, nil
}

// getPooledConn 获取池化连接.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	// 获取最后一个连接,并从 connPool 中移除.
	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// returnConn 将一个可复用的 conn 放回 connPool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns, _ := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

// InstallSnapshot 实现 Transport 接口.
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	// get a conn, always close for InstallSnapshot.
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer conn.Release()

	// set a deadline,depend on request size.
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	// send the RPC.
	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	// stream the state,使用流传输快照数据.
	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// flush.
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// decode the response, not return conn.
	if _, err = decodeResponse(conn, resp); err != nil {
		return err
	}

	return nil
}

// EncodePeer 实现 Transport 接口.
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer 实现 Transport 接口.
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// SetHeartbeatHandler 实现 Transport 接口.
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFuncLock.Lock()
	defer n.heartbeatFuncLock.Unlock()
	n.heartbeatFunc = cb
}

// ----- 作为 RPC client 发送 RPC 请求 -----
// sendRPC 用于编码和发送 RPC.
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// write the rpc type.
	if err := conn.w.WriteByte(rpcType); err != nil {
		return err
	}

	// send the request.
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// flush.
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}

// decodeResponse 解码 RPC 响应,并且返回 conn 是否可以被重复使用.
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// decode the error if have.
	var rpcErr string
	if err := conn.dec.Decode(rpcErr); err != nil {
		conn.Release()
		return false, err
	}

	// decode the response.
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// format an error if have.
	if rpcErr != "" {
		return true, fmt.Errorf(rpcErr)
	}
	return true, nil
}

// ----- 作为 RPC Server 接收并处理 RPC 请求 -----
// listen 监听并处理接收的 conn.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// accept incoming conn.
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// No error, reset loop delay.
		loopDelay = 0

		n.logger.Debug("accepted connection, local-address: ", n.LocalAddr(), "remote-address: ", conn.RemoteAddr().String())

		// handle the conn in routine.
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// handleConn 用于 conn 在其生命周期内处理事件.当 streamCtx 被 cancel 或者 conn 被关闭时,处理程序退出.
func (n *NetworkTransport) handleConn(streamCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	// Use Msgpack Schema-Free Encoding Format.
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-streamCtx.Done():
			n.logger.Debug("stream layer is closed.")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response", "error", err)
			return
		}
	}
}

// handleCommand 用于解码和分发单个命令,会将收到的命令请求封装成 RPC 通过 channel 传递给 raft-node 进行处理.
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	// create the rpc object.
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// decode the command.
	isHeartbeat := false
	switch rpcType {
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err = dec.Decode(req); err != nil {
			return err
		}
		rpc.Command = &req

		// check if this is heartbeat.
		if req.Term != 0 && req.Leader != nil &&
			req.PreLogIndex == 0 && req.PreLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

	case rpcRequestVote:
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)

	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	// check for heartbeat fast-path.
	if isHeartbeat {
		n.heartbeatFuncLock.Lock()
		fn := n.heartbeatFunc
		n.heartbeatFuncLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// dispatch the rpc,将封装好的 RPC 交给 raft-node 消费.
	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// wait for response,等待 raft-node 处理完后的 resp.
RESP:
	select {
	case resp := <-respCh:
		// send the error.
		var respErr string
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err = enc.Encode(respErr); err != nil {
			return err
		}

		// send the response.
		if err = enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

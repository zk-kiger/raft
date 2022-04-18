package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// NewInmemAddr 返回一个新的 UUID 随机的 in-memory 地址.
func NewInmemAddr() ServerAddress {
	return ServerAddress(generateUUID())
}

// InmemTransport 实现 Transport 接口,以允许 Raft 在内存中进行测试,而无需通过网络.
type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr  ServerAddress
	peers      map[ServerAddress]*InmemTransport
	timeout    time.Duration
}

// NewInmemTransportWithTimeout 用于初始化一个新的传输,如果没有指定,则生成一个随机的本地地址.
// 给定的超时将用于决定等待连接的节点处理我们发送给它的 RPC 所需的时间.
func NewInmemTransportWithTimeout(addr ServerAddress, timeout time.Duration) (ServerAddress, *InmemTransport) {
	if string(addr) == "" {
		addr = NewInmemAddr()
	}
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    timeout,
	}
	return addr, trans
}

func NewInmemTransport(addr ServerAddress) (ServerAddress, *InmemTransport) {
	return NewInmemTransportWithTimeout(addr, 500*time.Millisecond)
}

// SetHeartbeatHandler 将心跳处理程序作为一种 fast-path(快传),这个 transport 不支持.
func (i *InmemTransport) SetHeartbeatHandler(cb func(RPC)) {
}

// Consumer 实现 Transport 接口.
func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

// LocalAddr 实现 Transport 接口.
func (i *InmemTransport) LocalAddr() ServerAddress {
	return i.localAddr
}

// AppendEntries 实现 Transport 接口.
func (i *InmemTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// 复制,将结果返回.
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

// RequestVote 实现 Transport 接口.
func (i *InmemTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	// 复制,将结果返回.
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

// InstallSnapshot 实现 Transport 接口.
func (i *InmemTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, 10*i.timeout)
	if err != nil {
		return err
	}

	// 复制,将结果返回.
	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target ServerAddress, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	// Send the RPC over
	respCh := make(chan RPCResponse, 1)
	req := RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}
	select {
	case peer.consumerCh <- req:
	case <-time.After(timeout):
		err = fmt.Errorf("send timed out")
		return
	}

	// Wait for a response
	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = fmt.Errorf("command timed out")
	}
	return
}

// EncodePeer 实现 Transport 接口.
func (i *InmemTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	return []byte(p)
}

// DecodePeer 实现 Transport 接口.
func (i *InmemTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// Connect 用于将此传输连接到给定节点地址的另一个传输.只允许本地路由.
func (i *InmemTransport) Connect(peer ServerAddress, t Transport) {
	trans := t.(*InmemTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

// Disconnect 用于删除给定地址具有路由能力的节点.
func (i *InmemTransport) Disconnect(peer ServerAddress) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)
}

// DisconnectAll 移除所有具有路由能力的节点.
func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[ServerAddress]*InmemTransport)
}

// Close 用于永久地禁用传输.
func (i *InmemTransport) Close() error {
	i.DisconnectAll()
	return nil
}

package raft

import (
	"io"
)

// RPCResponse 捕获响应和错误.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC 具有命令,并且提供响应机制.
type RPC struct {
	Command  interface{}
	Reader   io.Reader // 只用于 InstallSnapshot 读取快照数据.
	RespChan chan<- RPCResponse
}

// Respond 用于回应 RPC 响应、错误.
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport 为网络传输提供接口,用于 Raft 与其他节点进行通信.
type Transport interface {
	// Consumer 当前节点返回一个 channel,作为消费者,消费并响应来自其他节点 RPC 请求.
	Consumer() <-chan RPC

	// LocalAddr 用于返回节点本地网络地址,便于区别于其他的节点.
	LocalAddr() ServerAddress

	// AppendEntries 向目标节点发送追加日志条目 RPC.
	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote 向目标节点发送请求投票 RPC.
	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot 用于将快照下推给 Follower,数据从 ReadCloser 中读取,被流式地传输给客户端(follower).
	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer 序列化节点地址.
	EncodePeer(id ServerID, addr ServerAddress) []byte

	// DecodePeer 反序列化节点地址.
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler 将心跳处理程序作为一种 fast-path(快传).为了避免磁盘 IO 的队头阻塞.
	// 如果 Transport 不支持该方法,则可以忽略调用,将心跳推给消费者 Channel.
	SetHeartbeatHandler(cb func(rpc RPC))
}

package raft

// RPCHeader 是一个常见的子结构,用于传递协议版本和其他有关集群的信息.
type RPCHeader struct {
	// ProtocolVersion 发送方所使用的协议版本.
	ProtocolVersion ProtocolVersion
}

// WithRPCHeader 接口,用于暴露 RPCHeader.
type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

// AppendEntriesRequest 用于日志复制时追加日志条目的命令.
type AppendEntriesRequest struct {
	RPCHeader

	// current term and leader.
	Term   uint64
	Leader []byte

	// 前一个日志条目,用于日志完整性检测.
	PrevLogIndex uint64
	PrevLogTerm  uint64

	// 待提交的新日志.
	Entries []*Log

	// Leader 提交的日志条目索引.
	LeaderCommitIndex uint64
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *AppendEntriesRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// AppendEntriesResponse 是 AppendEntriesRequest 的响应.
type AppendEntriesResponse struct {
	RPCHeader

	// Term 如果 Leader 已过时,使用较新的 Term,且 Leader -> Follower.
	Term uint64

	// LastLog 用于帮助加速慢节点的日志追加.
	LastLog uint64

	// Success 如果存在冲突日志,就不会成功.
	Success bool

	// NoRetryBackoff 在某些情况下,请求未成功,但无需等待下一次重试.
	NoRetryBackoff bool
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *AppendEntriesResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteRequest 用于在一轮选举中 Candidate 请求其他 Raft 节点为自己投票的命令.
type RequestVoteRequest struct {
	RPCHeader

	// current term and our id.
	Term      uint64
	Candidate []byte

	// ensure safety,选举安全限制.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *RequestVoteRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteResponse 是 RequestVoteRequest 的响应.
type RequestVoteResponse struct {
	RPCHeader

	// Term 如果 Leader 已过时,使用较新的 Term,且 Leader -> Follower.
	Term uint64

	// Granted 投票是否通过.
	Granted bool
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *RequestVoteResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotRequest 发送快照安装请求到其他 Raft 节点,用于快速重建 Raft 节点的日志(状态机).
// 通常发送给日志落后太多的 Follower.
type InstallSnapshotRequest struct {
	RPCHeader
	SnapshotVersion SnapshotVersion

	Term   uint64
	Leader []byte

	// 快照中最新的日志信息.
	LastLogIndex uint64
	LastLogTerm  uint64

	// 集群成员.
	Configuration []byte
	// 最初写入"配置"条目的日志索引.
	ConfigurationIndex uint64

	// 快照大小.
	Size int64
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *InstallSnapshotRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotResponse 是 InstallSnapshotRequest 的响应.
type InstallSnapshotResponse struct {
	RPCHeader

	Term    uint64
	Success bool
}

// GetRPCHeader 实现 WithRPCHeader 接口.
func (r *InstallSnapshotResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

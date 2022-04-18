package raft

import "fmt"

// ServerSuffrage 决定 Configuration 中 Server 是否获得投票.
type ServerSuffrage int

// Note: 不要重新编号,这些数字已经记录到日志中.
const (
	// Voter 是一个服务器,其投票被计入选举,其匹配索引用于推进 leader 的提交索引.
	Voter ServerSuffrage = iota
	// Nonvoter 是接收日志条目但不考虑用于选举或 commitment(仅 leader 记录) 的服务器.
	Nonvoter
	// Staging (暂存)是一个像 nonvoter 一样的服务器,但有一个例外：
	// 一旦 Staging 服务器接收到足够多的日志条目以充分赶上 leader 的日志,
	// leader 将调用成员资格更改,将 Staging 服务器更改为 Voter.
	Staging
)

func (s ServerSuffrage) String() string {
	switch s {
	case Voter:
		return "Voter"
	case Nonvoter:
		return "Nonvoter"
	case Staging:
		return "Staging"
	}
	return "ServerSuffrage"
}

// ConfigurationStore 提供了一个接口，可以选择由 FSM 实现，以存储在复制日志中所做的配置更新.
// 通常来说,只对改变持久状态的 FSM 是必须的,而不是应用更改或定期快照的 FSM.
// 通过存储 configurations changes,持久的 FSM 状态可以表现为一个完整的快照,可以在没有外部快照
// 的情况下恢复,仅用于 raft configuration 持久化.
type ConfigurationStore interface {
	FSM

	// StoreConfiguration 一旦提交了 configuration change log entry,就会调用该方法.
	// 它需要配置的索引和配置值.
	StoreConfiguration(index uint64, configuration Configuration)
}

// ServerID 是始终标识节点的唯一字符串.
type ServerID string

// ServerAddress 是可以传输联系的服务器的网络地址.
type ServerAddress string

// Server 追踪单个服务节点的配置信息.
type Server struct {
	// Suffrage 决定节点是否获得投票.
	Suffrage ServerSuffrage
	// ID 始终标识节点的唯一字符串.
	ID ServerID
	// Address 节点可以连接的网络地址.
	Address ServerAddress
}

// Configuration 跟踪集群中有哪些服务器,以及它们是否有投票权.
// 应该包括本地服务器,如果它是集群的成员的话.服务器没有按特定顺序列出,
// 但每个服务器应该只出现一次.这些条目会在成员资格更改期间附加到日志中.
type Configuration struct {
	Servers []Server
}

// Clone 生成 Configuration 的深拷贝副本.
func (c *Configuration) Clone() (copy Configuration) {
	copy.Servers = append(copy.Servers, c.Servers...)
	return
}

// ConfigurationChangeCommand 用于以不同的方式改变集群配置.
type ConfigurationChangeCommand uint8

const (
	// AddStaging 使服务器成为 Staging,除非它是 Voter.
	AddStaging ConfigurationChangeCommand = iota
	// AddNonvoter 使服务器成为 Nonvoter,除非它是 Staging or Voter.
	AddNonvoter
	// DemoteVoter 使服务器成为 Nonvoter,除非它不存在.
	DemoteVoter
	// RemoveServer 从集群成员中完全删除服务器.
	RemoveServer
	// Promote 由 leader 自动创建;它将 Staging 更改为 Voter.
	Promote
)

func (c ConfigurationChangeCommand) String() string {
	switch c {
	case AddStaging:
		return "AddStaging"
	case AddNonvoter:
		return "AddNonvoter"
	case DemoteVoter:
		return "DemoteVoter"
	case RemoveServer:
		return "RemoveServer"
	case Promote:
		return "Promote"
	}
	return "ConfigurationChangeCommand"
}

// configurationChangeRequest 描述了 leader 想要对其当前配置进行的更改.
// 它仅在单个服务器中使用(从不序列化到日志中),作为 'configurationChangeFuture' 的一部分.
type configurationChangeRequest struct {
	command       ConfigurationChangeCommand
	serverID      ServerID
	serverAddress ServerAddress // 仅适用于 AddStaging, AddNonvoter.
	// prevIndex,如果非零,是可以应用此更改的唯一配置的索引;
	// 如果同时添加了另一个配置条目,则此请求将失败.
	prevIndex uint64
}

// configurations 在每台服务器上对其配置进行状态跟踪.
// 仅存储两个配置的缺点是,如果在状态机尚未应用 commitIndex 的情况下尝试拍摄快照,
// 我们没有记录在逻辑上适合该快照的配置.现在不允许在这种情况下使用快照.
// 另一种方法是跟踪日志中的每个配置更改.
type configurations struct {
	// committed 是已提交的 log/snapshot 中的最新配置(索引最大的配置).
	committed Configuration
	// committedIndex 是写入已提交日志的索引.
	committedIndex uint64
	// latest 是 log/snapshot 中的最新配置(maybe committed or uncommitted).
	latest Configuration
	// latestIndex 是写入最新日志的索引.
	latestIndex uint64
}

// Clone 为 configurations 生成一个深拷贝对象.
func (c *configurations) Clone() (copy configurations) {
	copy.committed = c.committed.Clone()
	copy.committedIndex = c.committedIndex
	copy.latest = c.latest.Clone()
	copy.latestIndex = c.latestIndex
	return
}

// hasVote 如果由 'id' 标识的服务器是提供的配置中的投票者,返回 true.
func hasVote(configuration Configuration, id ServerID) bool {
	for _, server := range configuration.Servers {
		if server.ID == id {
			return server.Suffrage == Voter
		}
	}
	return false
}

// EncodeConfiguration 使用 MsgPack 序列化 Configuration,否则 panic.
func EncodeConfiguration(configuration Configuration) []byte {
	buf, err := encodeMsgPack(configuration)
	if err != nil {
		panic(fmt.Errorf("failed to encode configuration: %v", err))
	}
	return buf.Bytes()
}

// DecodeConfiguration 使用 MsgPack 反序列化 Configuration,否则 panic.
func DecodeConfiguration(buf []byte) Configuration {
	var configuration Configuration
	if err := decodeMsgPack(buf, &configuration); err != nil {
		panic(fmt.Errorf("failed to decode configuration: %v", err))
	}
	return configuration
}

// checkConfiguration 测试集群成员配置.
func checkConfiguration(configuration Configuration) error {
	idSet := make(map[ServerID]bool)
	addressSet := make(map[ServerAddress]bool)
	var voters int
	for _, server := range configuration.Servers {
		if server.ID == "" {
			return fmt.Errorf("empty ID in configuration: %v", configuration)
		}
		if server.Address == "" {
			return fmt.Errorf("empty address in configuration: %v", server)
		}
		if idSet[server.ID] {
			return fmt.Errorf("found duplicate ID in configuration: %v", server.ID)
		}
		idSet[server.ID] = true
		if addressSet[server.Address] {
			return fmt.Errorf("found duplicate address in configuration: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("need at least one voter in configuration: %v", configuration)
	}
	return nil
}

// nextConfiguration 从当前配置和配置更改请求生成一个新配置.
// 它从 appendConfigurationEntry 中分离出来,以便可以轻松地进行单元测试.
func nextConfiguration(current Configuration, currentIndex uint64, change configurationChangeRequest) (Configuration, error) {
	if change.prevIndex > 0 && change.prevIndex != currentIndex {
		return Configuration{}, fmt.Errorf("configuration changed since %v (latest is %v)", change.prevIndex, currentIndex)
	}

	configuration := current.Clone()
	switch change.command {
	case AddStaging:
		newServer := Server{
			// TODO 这应该将服务器添加为 Staging,以便稍后自动提升为 Voter.
			//  然而,对 Voter 的提升还没有实现,对于今天的领导者循环与复制 goroutine 的协调方式,
			//  这样做并不是一件容易的事.所以,就目前而言,服务器将立即进行投票,并且下面的提升案例未使用.
			Suffrage: Voter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				if server.Suffrage == Voter {
					configuration.Servers[i].Address = change.serverAddress
				} else {
					configuration.Servers[i] = newServer
				}
				found = true
				break
			}
		}
		if !found {
			configuration.Servers = append(configuration.Servers, newServer)
		}
	case AddNonvoter:
		newServer := Server{
			Suffrage: Nonvoter,
			ID:       change.serverID,
			Address:  change.serverAddress,
		}
		found := false
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				if server.Suffrage != Nonvoter {
					configuration.Servers[i].Address = change.serverAddress
				} else {
					configuration.Servers[i] = newServer
				}
				found = true
				break
			}
		}
		if !found {
			configuration.Servers = append(configuration.Servers, newServer)
		}
	case DemoteVoter:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				configuration.Servers[i].Suffrage = Nonvoter
				break
			}
		}
	case RemoveServer:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID {
				configuration.Servers = append(configuration.Servers[:i], configuration.Servers[i+1:]...)
				break
			}
		}
	case Promote:
		for i, server := range configuration.Servers {
			if server.ID == change.serverID && server.Suffrage == Staging {
				configuration.Servers[i].Suffrage = Voter
				break
			}
		}
	}

	// make sure don't do something bad like remove last voter.
	if err := checkConfiguration(configuration); err != nil {
		return Configuration{}, err
	}

	return configuration, nil
}

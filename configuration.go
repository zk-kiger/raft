package raft

type ServerSuffrage int

type ServerID string

type ServerAddress string

type Server struct {
	// Suffrage 决定节点是否获得投票.
	Suffrage ServerSuffrage
	// ID 始终标识节点的唯一字符串.
	ID      ServerID
	// Address 节点可以连接的网络地址.
	Address ServerAddress
}

type Configuration struct {
	Servers []Server
}
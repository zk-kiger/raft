package raft

type ServerID string

type ServerAddress string

type Server struct {
	ID      ServerID
	Address ServerAddress
}

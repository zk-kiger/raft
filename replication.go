package raft

import "errors"

var (
	// ErrLogNotFound 给定的日志条目不可用.
	ErrLogNotFound = errors.New("log not found")
)

type followerReplication struct {

}

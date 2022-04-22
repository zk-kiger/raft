package raft

import (
	"reflect"
	"testing"
	"time"
)

type testAddrProvider struct {
	addr string
}

func (t *testAddrProvider) ServerAddress(id ServerID) (ServerAddress, error) {
	return ServerAddress(t.addr), nil
}

func makeTransport(t *testing.T, useAddrProvider bool, addressOverride string) (*NetworkTransport, error) {
	if useAddrProvider {
		config := &NetworkTransportConfig{MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t), ServerAddressProvider: &testAddrProvider{addressOverride}}
		return NewTCPTransportWithConfig("localhost:0", nil, config)
	}
	return NewTCPTransportWithLogger("localhost:0", nil, 2, time.Second, newTestLogger(t))
}

func TestNetworkTransport_AppendEntries(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		// Transport 1 is consumer
		trans1, err := makeTransport(t, useAddrProvider, "localhost:0")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans1.Close()
		rpcCh := trans1.Consumer()

		// Make the RPC request
		args := AppendEntriesRequest{
			Term:         10,
			Leader:       []byte("cartman"),
			PrevLogIndex: 100,
			PrevLogTerm:  4,
			Entries: []*Log{
				{
					Index: 101,
					Term:  4,
					Type:  LogNoop,
				},
			},
			LeaderCommitIndex: 90,
		}
		resp := AppendEntriesResponse{
			Term:    4,
			LastLog: 90,
			Success: true,
		}

		// Listen for a request
		go func() {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}

				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				t.Errorf("timeout")
			}
		}()

		// Transport 2 makes outbound request
		trans2, err := makeTransport(t, useAddrProvider, string(trans1.LocalAddr()))
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer trans2.Close()

		var out AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}

	}
}

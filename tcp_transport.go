package raft

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"io"
	"net"
	"time"
)

var (
	ErrNotPublished = errors.New("local bind address is not published")
	ErrNotTCP          = errors.New("local address is not a TCP address")
)

type TCPStreamLayer struct {
	// publish 可发布的地址.
	publish  net.Addr
	listener *net.TCPListener
}

// NewTCPTransport 返回一个建立在 TCP 流传输层之上的 NetworkTransport.
func NewTCPTransport(bindAddr string, publish net.Addr, maxPool int,
	timeout time.Duration, logOutput io.Writer) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, publish, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger 返回一个建立在 TCP 流传输层之上的 NetworkTransport,日志输出到 logger.
func NewTCPTransportWithLogger(bindAddr string, publish net.Addr, maxPool int,
	timeout time.Duration, logger hclog.Logger) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, publish, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig 返回一个建立在 TCP 流传输层之上的 NetworkTransport,使用给定的 Config 结构.
func NewTCPTransportWithConfig(bindAddr string, publish net.Addr,
	config *NetworkTransportConfig) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, publish, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

func newTCPTransport(bindAddr string, publish net.Addr,
	transportCreator func(stream StreamLayer) *NetworkTransport) (*NetworkTransport, error) {
	// try to bind addr.
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	// create stream.
	stream := &TCPStreamLayer{
		publish: publish,
		listener: listener.(*net.TCPListener),
	}

	// 校验 tcp 地址可用.
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		listener.Close()
		return nil, ErrNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, ErrNotPublished
	}

	// create network transport.
	networkTransport := transportCreator(stream)
	return networkTransport, nil
}

// Dial 实现 StreamLayer 接口.
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept 实现 net.Listener 接口.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close 实现 net.Listener 接口.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr 实现 net.Listener 接口.
func (t *TCPStreamLayer) Addr() net.Addr {
	// 使用已发布的地址.
	if t.publish != nil {
		return t.publish
	}
	return t.listener.Addr()
}

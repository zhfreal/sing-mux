package mux

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type protocolConn struct {
	net.Conn
	access         sync.Mutex
	request        Request
	requestWritten atomic.Bool
}

func newProtocolConn(conn net.Conn, request Request) net.Conn {
	writer, isVectorised := bufio.CreateVectorisedWriter(conn)
	if isVectorised {
		return &vectorisedProtocolConn{
			protocolConn{
				Conn:    conn,
				request: request,
			},
			writer,
		}
	} else {
		return &protocolConn{
			Conn:    conn,
			request: request,
		}
	}
}

func (c *protocolConn) NeedHandshake() bool {
	return !c.requestWritten.Load()
}

func (c *protocolConn) Write(p []byte) (n int, err error) {
	if c.requestWritten.Load() {
		return c.Conn.Write(p)
	}
	c.access.Lock()
	if c.requestWritten.Load() {
		c.access.Unlock()
		return c.Conn.Write(p)
	}
	buffer := EncodeRequest(c.request, p)
	_, err = c.Conn.Write(buffer.Bytes())
	buffer.Release()
	if err == nil {
		c.requestWritten.Store(true)
		n = len(p)
	}
	c.access.Unlock()
	return n, err
}

func (c *protocolConn) Upstream() any {
	return c.Conn
}

type vectorisedProtocolConn struct {
	protocolConn
	writer N.VectorisedWriter
}

func (c *vectorisedProtocolConn) WriteVectorised(buffers []*buf.Buffer) error {
	if c.requestWritten.Load() {
		return c.writer.WriteVectorised(buffers)
	}
	c.access.Lock()
	if c.requestWritten.Load() {
		c.access.Unlock()
		return c.writer.WriteVectorised(buffers)
	}
	buffer := EncodeRequest(c.request, nil)
	allBuffers := make([]*buf.Buffer, 0, len(buffers)+1)
	allBuffers = append(allBuffers, buffer)
	allBuffers = append(allBuffers, buffers...)
	err := c.writer.WriteVectorised(allBuffers)
	if err == nil {
		c.requestWritten.Store(true)
	}
	c.access.Unlock()
	return err
}

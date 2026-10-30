package mux

import (
	"net"
	"sync"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
)

type protocolConn struct {
	net.Conn
	access         sync.Mutex
	request        Request
	requestWritten bool
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
	c.access.Lock()
	defer c.access.Unlock()
	return !c.requestWritten
}

func (c *protocolConn) Write(p []byte) (n int, err error) {
	c.access.Lock()
	if c.requestWritten {
		c.access.Unlock()
		return c.Conn.Write(p)
	}
	buffer := EncodeRequest(c.request, p)
	_, err = c.Conn.Write(buffer.Bytes())
	buffer.Release()
	if err == nil {
		c.requestWritten = true
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
	c.access.Lock()
	if c.requestWritten {
		c.access.Unlock()
		return c.writer.WriteVectorised(buffers)
	}
	c.requestWritten = true
	c.access.Unlock()
	buffer := EncodeRequest(c.request, nil)
	return c.writer.WriteVectorised(append([]*buf.Buffer{buffer}, buffers...))
}

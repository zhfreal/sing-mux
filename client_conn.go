package mux

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type clientConn struct {
	connMu sync.RWMutex
	conn   net.Conn
	closed bool // Prevents swapping if connection is closed during dial

	dialMu sync.Mutex // Serializes network dials

	stateMu          sync.Mutex // Protects concurrent access to handshake variables
	requestWriteCond *sync.Cond
	requestWriting   bool
	responseReadCond *sync.Cond
	responseReading  bool
	destination    M.Socksaddr
	requestWritten bool
	responseRead   bool

	client           *Client
	ctx              context.Context
	firstWriteBuffer []byte
	retryCount       int
}

func (c *clientConn) getConn() net.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *clientConn) lazyInit() {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.requestWriteCond == nil {
		c.requestWriteCond = sync.NewCond(&c.stateMu)
		c.responseReadCond = sync.NewCond(&c.stateMu)
	}
}

func (c *clientConn) NeedHandshake() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return !c.requestWritten
}

func (c *clientConn) readResponse() error {
	response, err := ReadStreamResponse(c.getConn())
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientConn) trySwapConnection(failedConn net.Conn) (swapped bool, ok bool) {
	c.connMu.RLock()
	if c.conn != failedConn {
		c.connMu.RUnlock()
		return false, true
	}
	c.connMu.RUnlock()

	c.dialMu.Lock()
	defer c.dialMu.Unlock()

	c.connMu.RLock()
	if c.conn != failedConn {
		c.connMu.RUnlock()
		return false, true
	}
	c.connMu.RUnlock()

	c.stateMu.Lock()
	if c.client == nil || c.retryCount >= 2 || c.responseRead {
		c.stateMu.Unlock()
		return false, false
	}
	c.retryCount++
	c.requestWritten = false
	client := c.client
	c.stateMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	newStream, err := client.openStream(ctx)
	if err != nil {
		return false, false
	}

	c.connMu.Lock()
	if c.closed {
		c.connMu.Unlock()
		newStream.Close()
		return false, false
	}
	c.conn.Close()
	c.conn = newStream
	c.connMu.Unlock()
	return true, true
}

func (c *clientConn) Read(b []byte) (n int, err error) {
	c.lazyInit()
	for {
		c.stateMu.Lock()
		for !c.responseRead && c.responseReading {
			c.responseReadCond.Wait()
		}
		if c.responseRead {
			c.stateMu.Unlock()
			return c.getConn().Read(b)
		}
		c.responseReading = true
		c.stateMu.Unlock()

		err = c.readResponse()

		c.stateMu.Lock()
		c.responseReading = false
		c.responseReadCond.Broadcast()
		if err == nil {
			c.responseRead = true
			c.firstWriteBuffer = nil
			c.stateMu.Unlock()
			return c.getConn().Read(b)
		}
		c.stateMu.Unlock()

		currentConn := c.getConn()
		c.stateMu.Lock()
		if c.client != nil {
			c.client.Reset()
		}
		c.stateMu.Unlock()

		swapped, ok := c.trySwapConnection(currentConn)
		if ok {
			if swapped {
				c.stateMu.Lock()
				writeBuf := c.firstWriteBuffer
				c.stateMu.Unlock()

				_, writeErr := c.write(writeBuf, true)
				if writeErr == nil {
					continue
				}
			} else {
				// The connection was swapped and firstWriteBuffer was already replayed
				// by the swapping thread. Return len(b), nil directly to prevent writing b twice.
				return len(b), nil
			}
		}
		return 0, err
	}
}

func (c *clientConn) Write(b []byte) (n int, err error) {
	return c.write(b, false)
}

func (c *clientConn) write(b []byte, isReplay bool) (n int, err error) {
	c.lazyInit()
	c.stateMu.Lock()
	if !isReplay && !c.responseRead && c.client != nil && c.retryCount < 2 {
		if len(c.firstWriteBuffer)+len(b) <= 4096 {
			c.firstWriteBuffer = append(c.firstWriteBuffer, b...)
		} else {
			c.client = nil
		}
	}
	c.stateMu.Unlock()

	writeBuf := b
	for {
		c.stateMu.Lock()
		for !c.requestWritten && c.requestWriting {
			c.requestWriteCond.Wait()
		}
		if c.requestWritten {
			c.stateMu.Unlock()
			currentConn := c.getConn()
			n, err = currentConn.Write(writeBuf)
			if err != nil {
				swapped, ok := c.trySwapConnection(currentConn)
				if ok {
					if swapped {
						c.stateMu.Lock()
						writeBuf = c.firstWriteBuffer
						c.stateMu.Unlock()
						isReplay = true
						continue
					} else {
						// The connection was swapped and firstWriteBuffer was already replayed
						// by the swapping thread. Return len(b), nil directly to prevent writing b twice.
						return len(b), nil
					}
				}
			}
			return n, err
		}
		c.requestWriting = true
		c.stateMu.Unlock()

		currentConn := c.getConn()
		request := StreamRequest{
			Network:     N.NetworkTCP,
			Destination: c.destination,
		}
		buffer := buf.NewSize(streamRequestLen(request) + len(writeBuf))
		err = EncodeStreamRequest(request, buffer)
		if err != nil {
			buffer.Release()
			c.stateMu.Lock()
			c.requestWriting = false
			c.requestWriteCond.Broadcast()
			c.stateMu.Unlock()
			return 0, err
		}
		buffer.Write(writeBuf)
		_, err = currentConn.Write(buffer.Bytes())
		buffer.Release()

		c.stateMu.Lock()
		c.requestWriting = false
		c.requestWriteCond.Broadcast()
		if err == nil {
			c.requestWritten = true
			c.stateMu.Unlock()
			return len(b), nil
		}
		c.stateMu.Unlock()

		swapped, ok := c.trySwapConnection(currentConn)
		if ok {
			if swapped {
				c.stateMu.Lock()
				writeBuf = c.firstWriteBuffer
				c.stateMu.Unlock()
				isReplay = true
				continue
			} else {
				// The connection was swapped and firstWriteBuffer was already replayed
				// by the swapping thread. Return len(b), nil directly to prevent writing b twice.
				return len(b), nil
			}
		}
		return 0, err
	}
}

func (c *clientConn) Close() error {
	c.connMu.Lock()
	c.closed = true
	conn := c.conn
	c.connMu.Unlock()

	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *clientConn) LocalAddr() net.Addr {
	return c.getConn().LocalAddr()
}

func (c *clientConn) RemoteAddr() net.Addr {
	return c.destination.TCPAddr()
}

func (c *clientConn) ReaderReplaceable() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.responseRead
}

func (c *clientConn) WriterReplaceable() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.requestWritten
}

func (c *clientConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientConn) SetDeadline(t time.Time) error {
	return c.getConn().SetDeadline(t)
}

func (c *clientConn) SetReadDeadline(t time.Time) error {
	return c.getConn().SetReadDeadline(t)
}

func (c *clientConn) SetWriteDeadline(t time.Time) error {
	return c.getConn().SetWriteDeadline(t)
}

func (c *clientConn) SyscallConn() (syscall.RawConn, error) {
	if sc, ok := c.getConn().(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, syscall.ENOTSUP
}

var _ N.NetPacketConn = (*clientPacketConn)(nil)

type clientPacketConn struct {
	N.AbstractConn
	conn            N.ExtendedConn
	access          sync.Mutex
	destination     M.Socksaddr
	requestWritten  bool
	responseRead    bool
	readWaitOptions N.ReadWaitOptions
}

func (c *clientPacketConn) NeedHandshake() bool {
	return !c.requestWritten
}

func (c *clientPacketConn) readResponse() error {
	response, err := ReadStreamResponse(c.conn)
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientPacketConn) Read(b []byte) (n int, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(b) < int(length) {
		return 0, io.ErrShortBuffer
	}
	return io.ReadFull(c.conn, b[:length])
}

func (c *clientPacketConn) writeRequest(payload []byte) (n int, err error) {
	request := StreamRequest{
		Network:     N.NetworkUDP,
		Destination: c.destination,
	}
	rLen := streamRequestLen(request)
	if len(payload) > 0 {
		rLen += 2 + len(payload)
	}
	buffer := buf.NewSize(rLen)
	defer buffer.Release()
	err = EncodeStreamRequest(request, buffer)
	if err != nil {
		return
	}
	if len(payload) > 0 {
		common.Must(
			binary.Write(buffer, binary.BigEndian, uint16(len(payload))),
			common.Error(buffer.Write(payload)),
		)
	}
	_, err = c.conn.Write(buffer.Bytes())
	if err != nil {
		return
	}
	c.requestWritten = true
	return len(payload), nil
}

func (c *clientPacketConn) Write(b []byte) (n int, err error) {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.requestWritten {
		return c.writeRequest(b)
	}
	err = binary.Write(c.conn, binary.BigEndian, uint16(len(b)))
	if err != nil {
		return
	}
	return c.conn.Write(b)
}

func (c *clientPacketConn) ReadBuffer(buffer *buf.Buffer) (err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	return
}

func (c *clientPacketConn) WriteBuffer(buffer *buf.Buffer) error {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.requestWritten {
		defer buffer.Release()
		return common.Error(c.writeRequest(buffer.Bytes()))
	}
	bLen := buffer.Len()
	binary.BigEndian.PutUint16(buffer.ExtendHeader(2), uint16(bLen))
	return c.conn.WriteBuffer(buffer)
}

func (c *clientPacketConn) FrontHeadroom() int {
	return 2
}

func (c *clientPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.conn, p[:length])
	return
}

func (c *clientPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.requestWritten {
		return c.writeRequest(p)
	}
	err = binary.Write(c.conn, binary.BigEndian, uint16(len(p)))
	if err != nil {
		return
	}
	return c.conn.Write(p)
}

func (c *clientPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	err = c.ReadBuffer(buffer)
	return
}

func (c *clientPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	return c.WriteBuffer(buffer)
}

func (c *clientPacketConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *clientPacketConn) RemoteAddr() net.Addr {
	return c.destination.UDPAddr()
}

func (c *clientPacketConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientPacketConn) Upstream() any {
	return c.conn
}

var _ N.NetPacketConn = (*clientPacketAddrConn)(nil)

type clientPacketAddrConn struct {
	N.AbstractConn
	conn            N.ExtendedConn
	access          sync.Mutex
	destination     M.Socksaddr
	requestWritten  bool
	responseRead    bool
	readWaitOptions N.ReadWaitOptions
}

func (c *clientPacketAddrConn) NeedHandshake() bool {
	return !c.requestWritten
}

func (c *clientPacketAddrConn) readResponse() error {
	response, err := ReadStreamResponse(c.conn)
	if err != nil {
		return err
	}
	if response.Status == statusError {
		return E.New("remote error: ", response.Message)
	}
	return nil
}

func (c *clientPacketAddrConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	destination, err := M.SocksaddrSerializer.ReadAddrPort(c.conn)
	if err != nil {
		return
	}
	if destination.IsFqdn() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.conn, p[:length])
	return
}

func (c *clientPacketAddrConn) writeRequest(payload []byte, destination M.Socksaddr) (n int, err error) {
	request := StreamRequest{
		Network:     N.NetworkUDP,
		Destination: c.destination,
		PacketAddr:  true,
	}
	rLen := streamRequestLen(request)
	if len(payload) > 0 {
		rLen += M.SocksaddrSerializer.AddrPortLen(destination) + 2 + len(payload)
	}
	buffer := buf.NewSize(rLen)
	defer buffer.Release()
	err = EncodeStreamRequest(request, buffer)
	if err != nil {
		return
	}
	if len(payload) > 0 {
		err = M.SocksaddrSerializer.WriteAddrPort(buffer, destination)
		if err != nil {
			return
		}
		common.Must(
			binary.Write(buffer, binary.BigEndian, uint16(len(payload))),
			common.Error(buffer.Write(payload)),
		)
	}
	_, err = c.conn.Write(buffer.Bytes())
	if err != nil {
		return
	}
	c.requestWritten = true
	return len(payload), nil
}

func (c *clientPacketAddrConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.requestWritten {
		return c.writeRequest(p, M.SocksaddrFromNet(addr))
	}
	err = M.SocksaddrSerializer.WriteAddrPort(c.conn, M.SocksaddrFromNet(addr))
	if err != nil {
		return
	}
	err = binary.Write(c.conn, binary.BigEndian, uint16(len(p)))
	if err != nil {
		return
	}
	return c.conn.Write(p)
}

func (c *clientPacketAddrConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	destination, err = M.SocksaddrSerializer.ReadAddrPort(c.conn)
	if err != nil {
		return
	}
	var length uint16
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	return
}

func (c *clientPacketAddrConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.requestWritten {
		defer buffer.Release()
		return common.Error(c.writeRequest(buffer.Bytes(), destination))
	}
	bLen := buffer.Len()
	header := buf.With(buffer.ExtendHeader(M.SocksaddrSerializer.AddrPortLen(destination) + 2))
	err := M.SocksaddrSerializer.WriteAddrPort(header, destination)
	if err != nil {
		return err
	}
	common.Must(binary.Write(header, binary.BigEndian, uint16(bLen)))
	return c.conn.WriteBuffer(buffer)
}

func (c *clientPacketAddrConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *clientPacketAddrConn) FrontHeadroom() int {
	return 2 + M.MaxSocksaddrLength
}

func (c *clientPacketAddrConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *clientPacketAddrConn) Upstream() any {
	return c.conn
}

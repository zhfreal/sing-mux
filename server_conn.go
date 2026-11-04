package mux

import (
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/rw"
)

type serverConn struct {
	N.ExtendedConn
	access          sync.Mutex
	responseWritten bool
}

func (c *serverConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.responseWritten
}

func (c *serverConn) HandshakeFailure(err error) error {
	c.access.Lock()
	defer c.access.Unlock()
	errMessage := err.Error()
	buffer := buf.NewSize(ResponseStatusHeaderLen + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverConn) Write(b []byte) (n int, err error) {
	c.access.Lock()
	if c.responseWritten {
		c.access.Unlock()
		return c.ExtendedConn.Write(b)
	}
	buffer := buf.NewSize(ResponseStatusHeaderLen + len(b))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusSuccess),
		common.Error(buffer.Write(b)),
	)
	_, err = c.ExtendedConn.Write(buffer.Bytes())
	if err != nil {
		c.access.Unlock()
		return
	}
	c.responseWritten = true
	c.access.Unlock()
	return len(b), nil
}

func (c *serverConn) WriteBuffer(buffer *buf.Buffer) error {
	c.access.Lock()
	if c.responseWritten {
		c.access.Unlock()
		return c.ExtendedConn.WriteBuffer(buffer)
	}
	if buffer.Start() < ResponseStatusHeaderLen {
		newBuf := buf.NewSize(buffer.Len() + ResponseStatusHeaderLen)
		newBuf.Resize(ResponseStatusHeaderLen, 0)
		common.Must1(newBuf.Write(buffer.Bytes()))
		buffer.Release()
		buffer = newBuf
	}
	buffer.ExtendHeader(ResponseStatusHeaderLen)[0] = statusSuccess
	c.responseWritten = true
	c.access.Unlock()
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverConn) FrontHeadroom() int {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.responseWritten {
		return ResponseStatusHeaderLen
	}
	return 0
}

func (c *serverConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverConn) Upstream() any {
	return c.ExtendedConn
}

type serverPacketConn struct {
	N.ExtendedConn
	access          sync.Mutex
	readAccess      sync.Mutex
	destination     M.Socksaddr
	responseWritten bool
}

func (c *serverPacketConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.responseWritten
}

func (c *serverPacketConn) HandshakeFailure(err error) error {
	c.access.Lock()
	defer c.access.Unlock()
	errMessage := err.Error()
	buffer := buf.NewSize(ResponseStatusHeaderLen + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	length, err := readUint16(c.ExtendedConn)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.ExtendedConn, int(length))
	if err != nil {
		return
	}
	destination = c.destination
	return
}

func (c *serverPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.access.Lock()
	defer c.access.Unlock()
	needed := PacketLengthHeaderLen
	if !c.responseWritten {
		needed += ResponseStatusHeaderLen
	}
	if buffer.Start() < needed {
		newBuf := buf.NewSize(buffer.Len() + needed)
		newBuf.Resize(needed, 0)
		common.Must1(newBuf.Write(buffer.Bytes()))
		buffer.Release()
		buffer = newBuf
	}
	pLen := buffer.Len()
	binary.BigEndian.PutUint16(buffer.ExtendHeader(PacketLengthHeaderLen), uint16(pLen))
	if !c.responseWritten {
		buffer.ExtendHeader(ResponseStatusHeaderLen)[0] = statusSuccess
		c.responseWritten = true
	}
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	length, err := readUint16(c.ExtendedConn)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		_, _ = io.CopyN(io.Discard, c.ExtendedConn, int64(length))
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.ExtendedConn, p[:length])
	return
}

func (c *serverPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.access.Lock()
	defer c.access.Unlock()
	needed := PacketLengthHeaderLen + len(p)
	if !c.responseWritten {
		needed += ResponseStatusHeaderLen
	}
	buffer := buf.NewSize(needed)
	defer buffer.Release()
	if !c.responseWritten {
		common.Must(buffer.WriteByte(statusSuccess))
	}
	binary.BigEndian.PutUint16(buffer.Extend(PacketLengthHeaderLen), uint16(len(p)))
	common.Must1(buffer.Write(p))
	_, err = c.ExtendedConn.Write(buffer.Bytes())
	if err != nil {
		return 0, err
	}
	c.responseWritten = true
	return len(p), nil
}

func (c *serverPacketConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverPacketConn) Upstream() any {
	return c.ExtendedConn
}

func (c *serverPacketConn) FrontHeadroom() int {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.responseWritten {
		return ResponseStatusHeaderLen + PacketLengthHeaderLen
	}
	return PacketLengthHeaderLen
}

type serverPacketAddrConn struct {
	N.ExtendedConn
	access          sync.Mutex
	readAccess      sync.Mutex
	responseWritten bool
}

func (c *serverPacketAddrConn) NeedHandshake() bool {
	c.access.Lock()
	defer c.access.Unlock()
	return !c.responseWritten
}

func (c *serverPacketAddrConn) HandshakeFailure(err error) error {
	c.access.Lock()
	defer c.access.Unlock()
	errMessage := err.Error()
	buffer := buf.NewSize(ResponseStatusHeaderLen + rw.UvarintLen(uint64(len(errMessage))) + len(errMessage))
	defer buffer.Release()
	common.Must(
		buffer.WriteByte(statusError),
		rw.WriteVString(buffer, errMessage),
	)
	return common.Error(c.ExtendedConn.Write(buffer.Bytes()))
}

func (c *serverPacketAddrConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	destination, err := M.SocksaddrSerializer.ReadAddrPort(c.ExtendedConn)
	if err != nil {
		return
	}
	if destination.IsFqdn() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	length, err := readUint16(c.ExtendedConn)
	if err != nil {
		return
	}
	if cap(p) < int(length) {
		_, _ = io.CopyN(io.Discard, c.ExtendedConn, int64(length))
		return 0, nil, io.ErrShortBuffer
	}
	n, err = io.ReadFull(c.ExtendedConn, p[:length])
	return
}

func (c *serverPacketAddrConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	c.access.Lock()
	defer c.access.Unlock()
	destination := M.SocksaddrFromNet(addr)
	addrPortLen := M.SocksaddrSerializer.AddrPortLen(destination)
	needed := PacketLengthHeaderLen + addrPortLen + len(p)
	if !c.responseWritten {
		needed += ResponseStatusHeaderLen
	}
	buffer := buf.NewSize(needed)
	defer buffer.Release()
	if !c.responseWritten {
		common.Must(buffer.WriteByte(statusSuccess))
	}
	err = M.SocksaddrSerializer.WriteAddrPort(buffer, destination)
	if err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint16(buffer.Extend(PacketLengthHeaderLen), uint16(len(p)))
	common.Must1(buffer.Write(p))
	_, err = c.ExtendedConn.Write(buffer.Bytes())
	if err != nil {
		return 0, err
	}
	c.responseWritten = true
	return len(p), nil
}

func (c *serverPacketAddrConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	destination, err = M.SocksaddrSerializer.ReadAddrPort(c.ExtendedConn)
	if err != nil {
		return
	}
	length, err := readUint16(c.ExtendedConn)
	if err != nil {
		return
	}
	_, err = buffer.ReadFullFrom(c.ExtendedConn, int(length))
	if err != nil {
		return
	}
	return
}

func (c *serverPacketAddrConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	c.access.Lock()
	defer c.access.Unlock()
	addrPortLen := M.SocksaddrSerializer.AddrPortLen(destination)
	needed := PacketLengthHeaderLen + addrPortLen
	if !c.responseWritten {
		needed += ResponseStatusHeaderLen
	}
	if buffer.Start() < needed {
		newBuf := buf.NewSize(buffer.Len() + needed)
		newBuf.Resize(needed, 0)
		common.Must1(newBuf.Write(buffer.Bytes()))
		buffer.Release()
		buffer = newBuf
	}
	pLen := buffer.Len()
	binary.BigEndian.PutUint16(buffer.ExtendHeader(PacketLengthHeaderLen), uint16(pLen))
	err := M.SocksaddrSerializer.WriteAddrPort(buf.With(buffer.ExtendHeader(addrPortLen)), destination)
	if err != nil {
		buffer.Release()
		return err
	}
	if !c.responseWritten {
		buffer.ExtendHeader(ResponseStatusHeaderLen)[0] = statusSuccess
		c.responseWritten = true
	}
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverPacketAddrConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverPacketAddrConn) Upstream() any {
	return c.ExtendedConn
}

func (c *serverPacketAddrConn) FrontHeadroom() int {
	c.access.Lock()
	defer c.access.Unlock()
	if !c.responseWritten {
		return ResponseStatusHeaderLen + PacketLengthHeaderLen + M.MaxSocksaddrLength
	}
	return PacketLengthHeaderLen + M.MaxSocksaddrLength
}

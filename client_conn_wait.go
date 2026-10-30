package mux

import (
	"github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

var _ N.PacketReadWaiter = (*clientPacketConn)(nil)

func (c *clientPacketConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readWaitOptions = options
	return false
}

func (c *clientPacketConn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
	if !c.responseRead {
		err = c.readResponse()
		if err != nil {
			return
		}
		c.responseRead = true
	}
	length, err := readUint16(c.conn)
	if err != nil {
		return
	}
	buffer = c.readWaitOptions.NewPacketBuffer()
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	c.readWaitOptions.PostReturn(buffer)
	return
}

var _ N.PacketReadWaiter = (*clientPacketAddrConn)(nil)

func (c *clientPacketAddrConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readWaitOptions = options
	return false
}

func (c *clientPacketAddrConn) WaitReadPacket() (buffer *buf.Buffer, destination M.Socksaddr, err error) {
	c.readAccess.Lock()
	defer c.readAccess.Unlock()
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
	length, err := readUint16(c.conn)
	if err != nil {
		return
	}
	buffer = c.readWaitOptions.NewPacketBuffer()
	_, err = buffer.ReadFullFrom(c.conn, int(length))
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	c.readWaitOptions.PostReturn(buffer)
	return
}

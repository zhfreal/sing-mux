package mux

import (
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"sync"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/rw"
)

const (
	kFirstPaddings       = 16
	MaxPaddingPayloadLen = 65535
	MinPaddingLength     = 256
	MaxRandomPaddingSpan = 512
	PaddingHeaderLength  = 4
	MaxHeadroomPadding   = 1024
)

type paddingConn struct {
	N.ExtendedConn
	writeAccess      sync.Mutex
	readPadding      int
	writePadding     int
	readRemaining    int
	paddingRemaining int
}

func newPaddingConn(conn net.Conn) net.Conn {
	writer, isVectorised := bufio.CreateVectorisedWriter(conn)
	if isVectorised {
		return &vectorisedPaddingConn{
			paddingConn{
				ExtendedConn: bufio.NewExtendedConn(conn),
			},
			writer,
		}
	} else {
		return &paddingConn{
			ExtendedConn: bufio.NewExtendedConn(conn),
		}
	}
}

func (c *paddingConn) Read(p []byte) (n int, err error) {
	for {
		if c.readRemaining > 0 {
			if len(p) > c.readRemaining {
				p = p[:c.readRemaining]
			}
			n, err = c.ExtendedConn.Read(p)
			if err != nil {
				return
			}
			c.readRemaining -= n
			return
		}
		if c.paddingRemaining > 0 {
			err = rw.SkipN(c.ExtendedConn, c.paddingRemaining)
			if err != nil {
				return
			}
			c.paddingRemaining = 0
		}
		if c.readPadding < kFirstPaddings {
			var paddingHdr [4]byte
			_, err = io.ReadFull(c.ExtendedConn, paddingHdr[:])
			if err != nil {
				return
			}
			originalDataSize := int(binary.BigEndian.Uint16(paddingHdr[:2]))
			paddingLen := int(binary.BigEndian.Uint16(paddingHdr[2:]))
			c.readPadding++
			c.paddingRemaining = paddingLen

			if originalDataSize == 0 {
				continue
			}

			if len(p) > originalDataSize {
				p = p[:originalDataSize]
			}
			n, err = c.ExtendedConn.Read(p)
			if err != nil {
				return
			}
			c.readRemaining = originalDataSize - n
			return
		}
		return c.ExtendedConn.Read(p)
	}
}

func (c *paddingConn) Write(p []byte) (n int, err error) {
	for pLen := len(p); pLen > 0; {
		var data []byte
		if pLen > MaxPaddingPayloadLen {
			data = p[:MaxPaddingPayloadLen]
			p = p[MaxPaddingPayloadLen:]
			pLen -= MaxPaddingPayloadLen
		} else {
			data = p
			pLen = 0
		}
		var writeN int
		writeN, err = c.write(data)
		n += writeN
		if err != nil {
			break
		}
	}
	return n, err
}

func (c *paddingConn) write(p []byte) (n int, err error) {
	c.writeAccess.Lock()
	if c.writePadding < kFirstPaddings {
		c.writePadding++
		paddingLen := MinPaddingLength + rand.Intn(MaxRandomPaddingSpan)
		buffer := buf.NewSize(PaddingHeaderLength + len(p) + paddingLen)
		defer buffer.Release()
		header := buffer.Extend(PaddingHeaderLength)
		binary.BigEndian.PutUint16(header[:2], uint16(len(p)))
		binary.BigEndian.PutUint16(header[2:], uint16(paddingLen))
		common.Must1(buffer.Write(p))
		buffer.Extend(paddingLen)
		_, err = c.ExtendedConn.Write(buffer.Bytes())
		c.writeAccess.Unlock()
		if err == nil {
			n = len(p)
		}
		return
	}
	c.writeAccess.Unlock()
	return c.ExtendedConn.Write(p)
}

func (c *paddingConn) ReadBuffer(buffer *buf.Buffer) error {
	for {
		p := buffer.FreeBytes()
		if c.readRemaining > 0 {
			if len(p) > c.readRemaining {
				p = p[:c.readRemaining]
			}
			n, err := c.ExtendedConn.Read(p)
			if err != nil {
				return err
			}
			c.readRemaining -= n
			buffer.Truncate(n)
			return nil
		}
		if c.paddingRemaining > 0 {
			err := rw.SkipN(c.ExtendedConn, c.paddingRemaining)
			if err != nil {
				return err
			}
			c.paddingRemaining = 0
		}
		if c.readPadding < kFirstPaddings {
			var paddingHdr [4]byte
			_, err := io.ReadFull(c.ExtendedConn, paddingHdr[:])
			if err != nil {
				return err
			}
			originalDataSize := int(binary.BigEndian.Uint16(paddingHdr[:2]))
			paddingLen := int(binary.BigEndian.Uint16(paddingHdr[2:]))
			c.readPadding++
			c.paddingRemaining = paddingLen

			if originalDataSize == 0 {
				continue
			}

			if len(p) > originalDataSize {
				p = p[:originalDataSize]
			}
			n, err := c.ExtendedConn.Read(p)
			if err != nil {
				return err
			}
			c.readRemaining = originalDataSize - n
			buffer.Truncate(n)
			return nil
		}
		return c.ExtendedConn.ReadBuffer(buffer)
	}
}

func (c *paddingConn) WriteBuffer(buffer *buf.Buffer) error {
	c.writeAccess.Lock()
	if c.writePadding < kFirstPaddings {
		bufferLen := buffer.Len()
		if bufferLen > MaxPaddingPayloadLen {
			c.writeAccess.Unlock()
			defer buffer.Release()
			return common.Error(c.Write(buffer.Bytes()))
		}
		c.writePadding++
		paddingLen := MinPaddingLength + rand.Intn(MaxRandomPaddingSpan)
		if buffer.Start() < PaddingHeaderLength || buffer.FreeLen() < paddingLen {
			newBuf := buf.NewSize(PaddingHeaderLength + bufferLen + paddingLen)
			newBuf.Resize(PaddingHeaderLength, 0)
			common.Must1(newBuf.Write(buffer.Bytes()))
			buffer.Release()
			buffer = newBuf
		}
		header := buffer.ExtendHeader(PaddingHeaderLength)
		binary.BigEndian.PutUint16(header[:2], uint16(bufferLen))
		binary.BigEndian.PutUint16(header[2:], uint16(paddingLen))
		buffer.Extend(paddingLen)
		err := c.ExtendedConn.WriteBuffer(buffer)
		c.writeAccess.Unlock()
		return err
	}
	c.writeAccess.Unlock()
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *paddingConn) FrontHeadroom() int {
	return PaddingHeaderLength + MinPaddingLength + MaxHeadroomPadding
}

func (c *paddingConn) Upstream() any {
	return c.ExtendedConn
}

type vectorisedPaddingConn struct {
	paddingConn
	writer N.VectorisedWriter
}

func (c *vectorisedPaddingConn) WriteVectorised(buffers []*buf.Buffer) error {
	c.writeAccess.Lock()
	if c.writePadding < kFirstPaddings {
		bufferLen := buf.LenMulti(buffers)
		if bufferLen > MaxPaddingPayloadLen {
			c.writeAccess.Unlock()
			defer buf.ReleaseMulti(buffers)
			for _, buffer := range buffers {
				_, err := c.Write(buffer.Bytes())
				if err != nil {
					return err
				}
			}
			return nil
		}
		c.writePadding++
		paddingLen := MinPaddingLength + rand.Intn(MaxRandomPaddingSpan)
		header := buf.NewSize(PaddingHeaderLength)
		hdrBytes := header.Extend(PaddingHeaderLength)
		binary.BigEndian.PutUint16(hdrBytes[:2], uint16(bufferLen))
		binary.BigEndian.PutUint16(hdrBytes[2:], uint16(paddingLen))
		padding := buf.NewSize(paddingLen)
		padding.Extend(paddingLen)
		buffers = append(append([]*buf.Buffer{header}, buffers...), padding)
		err := c.writer.WriteVectorised(buffers)
		c.writeAccess.Unlock()
		return err
	}
	c.writeAccess.Unlock()
	return c.writer.WriteVectorised(buffers)
}

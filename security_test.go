package mux

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func TestPaddingZeroedOnAllWritePaths(t *testing.T) {
	// Pollute pooled buffers across size classes with 0xA5 canary.
	for _, sz := range []int{256, 512, 1024, 2048} {
		var held []*buf.Buffer
		for i := 0; i < 16; i++ {
			b := buf.NewSize(sz)
			raw := b.Extend(sz)
			for j := range raw {
				raw[j] = 0xA5
			}
			held = append(held, b)
		}
		for _, b := range held {
			b.Release()
		}
	}

	checkZeroTrailer := func(t *testing.T, wire []byte, expectedPayload []byte) {
		t.Helper()
		if len(wire) < PaddingHeaderLength {
			t.Fatalf("short frame: %d", len(wire))
		}
		origLen := int(binary.BigEndian.Uint16(wire[:2]))
		padLen := int(binary.BigEndian.Uint16(wire[2:4]))
		if origLen != len(expectedPayload) {
			t.Fatalf("origLen=%d, want %d", origLen, len(expectedPayload))
		}
		if len(wire) != PaddingHeaderLength+origLen+padLen {
			t.Fatalf("wire len=%d, want %d", len(wire), PaddingHeaderLength+origLen+padLen)
		}
		if !bytes.Equal(wire[PaddingHeaderLength:PaddingHeaderLength+origLen], expectedPayload) {
			t.Fatalf("payload mismatch")
		}
		trailer := wire[PaddingHeaderLength+origLen:]
		for i, b := range trailer {
			if b != 0 {
				t.Fatalf("uninitialized padding byte at offset %d: 0x%02x", i, b)
			}
		}
	}

	// 1. paddingConn.Write
	var out1 bytes.Buffer
	pc1 := newPaddingConn(&pipeConn{reader: bytes.NewReader(nil), writer: &out1})
	if _, err := pc1.Write([]byte("hello-write")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	checkZeroTrailer(t, out1.Bytes(), []byte("hello-write"))

	// 2. paddingConn.WriteBuffer (zero headroom -> reallocated)
	var out2 bytes.Buffer
	pc2 := newPaddingConn(&pipeConn{reader: bytes.NewReader(nil), writer: &out2}).(*paddingConn)
	zb := buf.NewSize(16)
	_, _ = zb.WriteString("hello-wbuf")
	if err := pc2.WriteBuffer(zb); err != nil {
		t.Fatalf("WriteBuffer: %v", err)
	}
	checkZeroTrailer(t, out2.Bytes(), []byte("hello-wbuf"))

	// 3. vectorisedPaddingConn.WriteVectorised
	var out3 bytes.Buffer
	vConn := &secVectorisedPipe{writeBuf: &out3}
	vpc := newPaddingConn(vConn).(*vectorisedPaddingConn)
	vb := buf.NewSize(16)
	_, _ = vb.WriteString("hello-vec")
	if err := vpc.WriteVectorised([]*buf.Buffer{vb}); err != nil {
		t.Fatalf("WriteVectorised: %v", err)
	}
	checkZeroTrailer(t, out3.Bytes(), []byte("hello-vec"))
}

type secVectorisedPipe struct {
	net.Conn
	writeBuf *bytes.Buffer
}

func (s *secVectorisedPipe) Read(p []byte) (int, error)         { return 0, io.EOF }
func (s *secVectorisedPipe) Write(p []byte) (int, error)        { return s.writeBuf.Write(p) }
func (s *secVectorisedPipe) Close() error                       { return nil }
func (s *secVectorisedPipe) LocalAddr() net.Addr                { return nil }
func (s *secVectorisedPipe) RemoteAddr() net.Addr               { return nil }
func (s *secVectorisedPipe) SetDeadline(t time.Time) error      { return nil }
func (s *secVectorisedPipe) SetReadDeadline(t time.Time) error  { return nil }
func (s *secVectorisedPipe) SetWriteDeadline(t time.Time) error { return nil }
func (s *secVectorisedPipe) WriteVectorised(buffers []*buf.Buffer) error {
	defer buf.ReleaseMulti(buffers)
	for _, b := range buffers {
		if _, err := s.writeBuf.Write(b.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func TestBoundedErrorVStringResponses(t *testing.T) {
	// 0xFF x 9 + 0x01 encodes math.MaxUint64 (negative int64 -> makeslice panic in unpatched rw.ReadVString).
	maxUvarint := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	// 1 GiB uvarint (0x80 0x80 0x80 0x80 0x04)
	oneGiB := []byte{0x80, 0x80, 0x80, 0x80, 0x04}

	for _, varintBytes := range [][]byte{maxUvarint, oneGiB} {
		streamPayload := append([]byte{statusError}, varintBytes...)
		if _, err := ReadStreamResponse(bytes.NewReader(streamPayload)); err == nil {
			t.Fatalf("expected error from ReadStreamResponse on oversized varint")
		}
		brutalPayload := append([]byte{BrutalStatusError}, varintBytes...)
		if _, _, err := ReadBrutalResponse(bytes.NewReader(brutalPayload), true); err == nil {
			t.Fatalf("expected error from ReadBrutalResponse on oversized varint")
		}
	}
}

type secDelayedDialer struct {
	delay   time.Duration
	server  net.Conn
	dialed  atomic.Int32
	onDial  func(net.Conn)
}

func (d *secDelayedDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	idx := d.dialed.Add(1)
	if idx > 1 && d.delay > 0 {
		time.Sleep(d.delay)
	}
	c1, c2 := net.Pipe()
	go d.onDial(c2)
	return c1, nil
}

func (d *secDelayedDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

func TestClientConnConcurrentWriteDuringStreamSwap(t *testing.T) {
	var (
		mu       sync.Mutex
		received [][]byte
		wg       sync.WaitGroup
	)
	dialer := &secDelayedDialer{
		delay: 40 * time.Millisecond,
		onDial: func(serverConn net.Conn) {
			defer serverConn.Close()
			req, err := ReadRequest(serverConn)
			if err != nil {
				return
			}
			sess, err := newServerSession(serverConn, req.Protocol, DefaultH2MuxIdleTimeout, 0, 0, 0)
			if err != nil {
				return
			}
			defer sess.Close()
			stream, err := sess.Accept()
			if err != nil {
				return
			}
			defer stream.Close()
			_, err = ReadStreamRequest(stream)
			if err != nil {
				return
			}
			// First session closes before sending StreamResponse to trigger trySwapConnection.
			if len(received) == 0 {
				mu.Lock()
				first := len(received) == 0
				if first {
					received = append(received, []byte("EOF_FIRST"))
				}
				mu.Unlock()
				if first {
					_, _ = io.ReadAll(io.LimitReader(stream, 2))
					return
				}
			}
			// Second session responds OK and reads all replayed + subsequent bytes.
			_, _ = stream.Write([]byte{statusSuccess})
			data, _ := io.ReadAll(stream)
			mu.Lock()
			received = append(received, data)
			mu.Unlock()
			wg.Done()
		},
	}

	client, err := NewClient(Options{
		Dialer:               dialer,
		Protocol:             "smux",
		MaxConnections:       1,
		MaxStreamSwapRetries: 2,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	conn, err := client.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:80"))
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("W1")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	wg.Add(1)
	readErrCh := make(chan error, 1)
	readBuf := make([]byte, 16)
	go func() {
		_, rErr := conn.Read(readBuf)
		readErrCh <- rErr
	}()

	// Wait briefly until first session closes and Read enters trySwapConnection delay.
	time.Sleep(15 * time.Millisecond)
	if _, err := conn.Write([]byte("W2")); err != nil {
		t.Fatalf("concurrent write during swap: %v", err)
	}
	_ = conn.Close()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for swapped server stream")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Fatalf("expected 2 sessions, got %d", len(received))
	}
	if string(received[1]) != "W1W2" {
		t.Fatalf("expected exact W1W2 without duplication or loss, got %q", string(received[1]))
	}
	<-readErrCh
}

type noopServiceHandler struct{}

func (noopServiceHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	_ = conn.Close()
	return nil
}

func (noopServiceHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	_ = conn.Close()
	return nil
}

func TestServiceMaxStreamsAndHandshakeLimit(t *testing.T) {
	cConn, sConn := net.Pipe()
	defer cConn.Close()
	defer sConn.Close()

	svc, err := NewService(ServiceOptions{
		Handler:    noopServiceHandler{},
		MaxStreams: 2,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	svcErr := make(chan error, 1)
	go func() {
		svcErr <- svc.NewConnection(context.Background(), sConn, M.Metadata{})
	}()

	pConn := newProtocolConn(cConn, Request{Version: Version0, Protocol: ProtocolSmux})
	sess, err := newClientSession(pConn, ProtocolSmux, DefaultH2MuxIdleTimeout, 0, 0, 0)
	if err != nil {
		t.Fatalf("newClientSession: %v", err)
	}
	defer sess.Close()

	// Open 2 silent streams (no StreamRequest written) -> occupy MaxStreams=2.
	s1, err := sess.Open(time.Second)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	defer s1.Close()
	s2, err := sess.Open(time.Second)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer s2.Close()

	// 3rd stream should be immediately closed by server due to MaxStreams=2.
	s3, err := sess.Open(time.Second)
	if err != nil {
		t.Fatalf("open 3: %v", err)
	}
	defer s3.Close()
	_ = s3.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var b [1]byte
	if _, rErr := s3.Read(b[:]); rErr == nil {
		t.Fatalf("expected 3rd stream exceeding MaxStreams=2 to be closed")
	}

	_ = sess.Close()
	_ = cConn.Close()
	_ = bufio.NewExtendedConn
	select {
	case <-svcErr:
	case <-time.After(time.Second):
		t.Fatalf("service did not exit cleanly")
	}
}

type secFailingFirstWriteStream struct {
	net.Conn
	writeStarted chan struct{}
	unblockWrite chan struct{}
	once         sync.Once
}

func (s *secFailingFirstWriteStream) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.writeStarted) })
	<-s.unblockWrite
	return 0, errors.New("write failed on first stream")
}

func (s *secFailingFirstWriteStream) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (s *secFailingFirstWriteStream) Close() error { return nil }

func TestClientConnConcurrentWriteWhileRequestWritingSwapsCleanly(t *testing.T) {
	var (
		mu       sync.Mutex
		received []byte
	)
	wg := sync.WaitGroup{}
	wg.Add(1)

	dialer := &secDelayedDialer{
		delay: 30 * time.Millisecond,
		onDial: func(serverConn net.Conn) {
			defer serverConn.Close()
			req, err := ReadRequest(serverConn)
			if err != nil {
				return
			}
			sess, err := newServerSession(serverConn, req.Protocol, DefaultH2MuxIdleTimeout, 0, 0, 0)
			if err != nil {
				return
			}
			defer sess.Close()
			stream, err := sess.Accept()
			if err != nil {
				return
			}
			defer stream.Close()
			if _, err := ReadStreamRequest(stream); err != nil {
				return
			}
			_, _ = stream.Write([]byte{statusSuccess})
			data, _ := io.ReadAll(stream)
			mu.Lock()
			received = data
			mu.Unlock()
			wg.Done()
		},
	}

	client, err := NewClient(Options{
		Dialer:               dialer,
		Protocol:             "smux",
		MaxConnections:       1,
		MaxStreamSwapRetries: 2,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	failingStream := &secFailingFirstWriteStream{
		writeStarted: make(chan struct{}),
		unblockWrite: make(chan struct{}),
	}
	cc := &clientConn{
		conn:        failingStream,
		destination: M.ParseSocksaddr("127.0.0.1:80"),
		client:      client,
	}
	defer cc.Close()

	w1Done := make(chan error, 1)
	go func() {
		_, wErr := cc.Write([]byte("W1"))
		w1Done <- wErr
	}()

	<-failingStream.writeStarted

	w2Done := make(chan error, 1)
	go func() {
		_, wErr := cc.Write([]byte("W2"))
		w2Done <- wErr
	}()

	// Give W2 time to enter requestWriteCond.Wait() while W1 holds requestWriting=true.
	time.Sleep(15 * time.Millisecond)
	close(failingStream.unblockWrite)

	if err := <-w1Done; err != nil {
		t.Fatalf("W1 write failed after swap: %v", err)
	}
	if err := <-w2Done; err != nil {
		t.Fatalf("W2 write failed after swap: %v", err)
	}
	_ = cc.Close()

	select {
	case <-func() chan struct{} {
		ch := make(chan struct{})
		go func() { wg.Wait(); close(ch) }()
		return ch
	}():
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for swapped server stream")
	}

	mu.Lock()
	defer mu.Unlock()
	if string(received) != "W1W2" {
		t.Fatalf("expected exact W1W2 without duplication or loss, got %q", string(received))
	}
}

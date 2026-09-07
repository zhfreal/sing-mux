package mux

import (
	"context"
	"math"
	"net"
	"sync"
	"time"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/bufio"
	E "github.com/metacubex/sing/common/exceptions"
	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/x/list"
)

type Client struct {
	dialer           N.Dialer
	logger           logger.Logger
	protocol         byte
	maxConnections   int
	minStreams       int
	maxStreams       int
	padding          bool
	tcpTimeout       time.Duration
	access           sync.Mutex
	connections      list.List[abstractSession]
	brutal           BrutalOptions
	cMaxReuseTimes   Range
	hMaxRequestTimes Range
	hMaxReusableSecs Range
}

type Options struct {
	Dialer           N.Dialer
	Logger           logger.Logger
	Protocol         string
	MaxConnections   int
	MinStreams       int
	MaxStreams       int
	Padding          bool
	TCPTimeout       time.Duration
	Brutal           BrutalOptions
	CMaxReuseTimes   any
	HMaxRequestTimes any
	HMaxReusableSecs any
}

type BrutalOptions struct {
	Enabled    bool
	SendBPS    uint64
	ReceiveBPS uint64
}

func NewClient(options Options) (*Client, error) {
	client := &Client{
		dialer:           options.Dialer,
		logger:           options.Logger,
		maxConnections:   options.MaxConnections,
		minStreams:       options.MinStreams,
		maxStreams:       options.MaxStreams,
		padding:          options.Padding,
		tcpTimeout:       options.TCPTimeout,
		brutal:           options.Brutal,
		cMaxReuseTimes:   parseRange(options.CMaxReuseTimes),
		hMaxRequestTimes: parseRange(options.HMaxRequestTimes),
		hMaxReusableSecs: parseRange(options.HMaxReusableSecs),
	}
	if client.dialer == nil {
		client.dialer = N.SystemDialer
	}
	if client.maxStreams == 0 && client.maxConnections == 0 {
		client.minStreams = 8
	}
	if client.tcpTimeout == 0 {
		client.tcpTimeout = 500 * time.Millisecond
	}
	switch options.Protocol {
	case "", "h2mux":
		client.protocol = ProtocolH2Mux
	case "smux":
		client.protocol = ProtocolSmux
	case "yamux":
		client.protocol = ProtocolYAMux
	default:
		return nil, E.New("unknown protocol: " + options.Protocol)
	}
	return client, nil
}

func (c *Client) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		stream, err := c.openStream(ctx)
		if err != nil {
			return nil, err
		}
		return &clientConn{
			conn:        stream,
			destination: destination,
			client:      c,
			ctx:         ctx,
		}, nil
	case N.NetworkUDP:
		stream, err := c.openStream(ctx)
		if err != nil {
			return nil, err
		}
		extendedConn := bufio.NewExtendedConn(stream)
		return &clientPacketConn{AbstractConn: extendedConn, conn: extendedConn, destination: destination}, nil
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (c *Client) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	stream, err := c.openStream(ctx)
	if err != nil {
		return nil, err
	}
	extendedConn := bufio.NewExtendedConn(stream)
	return &clientPacketAddrConn{AbstractConn: extendedConn, conn: extendedConn, destination: destination}, nil
}

func (c *Client) openStream(ctx context.Context) (net.Conn, error) {
	var (
		session abstractSession
		stream  net.Conn
		err     error
	)
	for attempts := 0; attempts < 2; attempts++ {
		session, err = c.offer(ctx)
		if err != nil {
			continue
		}
		stream, err = session.Open(c.tcpTimeout)
		if err != nil {
			if w, ok := session.(*sessionWrapper); ok {
				w.activeStreams.Add(-1)
				c.maybeCloseSession(w, 0)
			}
			session.Close()
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	var wrapper *sessionWrapper
	if w, ok := session.(*sessionWrapper); ok {
		wrapper = w
		wrapper.ConsumeRequest()
	}
	ws := &wrapStream{
		Conn: stream,
		onClose: func() {
			if wrapper != nil {
				wrapper.activeStreams.Add(-1)
				c.maybeCloseSession(wrapper, 0)
			}
		},
	}
	if c.logger != nil && wrapper != nil {
		c.logger.Debug("sing-mux: opened stream, leftRequests=", wrapper.leftRequests.Load(), " leftReuseTimes=", wrapper.leftReuseTimes.Load())
	}
	return ws, nil
}

func (c *Client) offer(ctx context.Context) (abstractSession, error) {
	c.access.Lock()
	defer c.access.Unlock()

	now := time.Now()
	var sessions []abstractSession
	for element := c.connections.Front(); element != nil; {
		sess := element.Value
		if element.Value.IsClosed() {
			element.Value.Close()
			nextElement := element.Next()
			c.connections.Remove(element)
			element = nextElement
			continue
		}
		if wrapper, ok := sess.(*sessionWrapper); ok && wrapper.isRetired(now) {
			if wrapper.activeStreams.Load() == 0 && wrapper.NumStreams() == 0 {
				wrapper.Close()
				nextElement := element.Next()
				c.connections.Remove(element)
				element = nextElement
				continue
			}
			element = element.Next()
			continue
		}
		sessions = append(sessions, element.Value)
		element = element.Next()
	}
	if c.brutal.Enabled {
		if len(sessions) > 0 {
			sess := sessions[0]
			if wrapper, ok := sess.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return sess, nil
		}
		return c.offerNew(ctx)
	}
	session := common.MinBy(common.Filter(sessions, abstractSession.CanTakeNewRequest), abstractSession.NumStreams)
	if session == nil {
		return c.offerNew(ctx)
	}
	numStreams := session.NumStreams()
	if numStreams == 0 {
		if wrapper, ok := session.(*sessionWrapper); ok {
			wrapper.activeStreams.Add(1)
			wrapper.ConsumeReuse()
		}
		return session, nil
	}
	if c.maxConnections > 0 {
		if len(sessions) >= c.maxConnections || numStreams < c.minStreams {
			if wrapper, ok := session.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return session, nil
		}
	} else {
		if c.maxStreams > 0 && numStreams < c.maxStreams {
			if wrapper, ok := session.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return session, nil
		}
	}
	return c.offerNew(ctx)
}

func (c *Client) offerNew(ctx context.Context) (abstractSession, error) {
	ctx, cancel := context.WithTimeout(ctx, c.tcpTimeout)
	defer cancel()
	conn, err := c.dialer.DialContext(ctx, N.NetworkTCP, Destination)
	if err != nil {
		return nil, err
	}
	var version byte
	if c.padding {
		version = Version1
	} else {
		version = Version0
	}
	conn = newProtocolConn(conn, Request{
		Version:  version,
		Protocol: c.protocol,
		Padding:  c.padding,
	})
	if c.padding {
		conn = newPaddingConn(conn)
	}
	rawSession, err := newClientSession(conn, c.protocol)
	if err != nil {
		conn.Close()
		return nil, err
	}
	now := time.Now()
	wrapper := &sessionWrapper{
		abstractSession: rawSession,
		createdAt:       now,
	}
	wrapper.activeStreams.Store(1)
	wrapper.leftRequests.Store(math.MaxInt32)
	if req := c.hMaxRequestTimes.Rand(); req > 0 {
		wrapper.leftRequests.Store(int32(req))
	}
	wrapper.leftReuseTimes.Store(-1)
	if reuse := c.cMaxReuseTimes.Rand(); reuse > 0 {
		wrapper.leftReuseTimes.Store(int32(reuse))
	}
	if secs := c.hMaxReusableSecs.Rand(); secs > 0 {
		wrapper.unreusableAt = now.Add(time.Duration(secs) * time.Second)
		wrapper.timer = time.AfterFunc(time.Duration(secs)*time.Second, func() {
			c.maybeCloseSession(wrapper, 0)
		})
	}
	session := wrapper
	if c.brutal.Enabled {
		err = c.brutalExchange(ctx, conn, session)
		if err != nil {
			conn.Close()
			session.Close()
			return nil, E.Cause(err, "brutal exchange")
		}
	}
	c.connections.PushBack(session)
	return session, nil
}

func (c *Client) brutalExchange(ctx context.Context, sessionConn net.Conn, session abstractSession) error {
	stream, err := session.Open(c.tcpTimeout)
	if err != nil {
		return err
	}
	conn := &clientConn{conn: &wrapStream{Conn: stream}, destination: M.Socksaddr{Fqdn: BrutalExchangeDomain}}
	err = WriteBrutalRequest(conn, c.brutal.ReceiveBPS)
	if err != nil {
		return err
	}
	serverReceiveBPS, err := ReadBrutalResponse(conn)
	if err != nil {
		return err
	}
	conn.Close()
	sendBPS := c.brutal.SendBPS
	if serverReceiveBPS < sendBPS {
		sendBPS = serverReceiveBPS
	}
	clientBrutalErr := SetBrutalOptions(sessionConn, sendBPS)
	if clientBrutalErr != nil {
		if c.logger != nil {
			c.logger.Debug(E.Cause(clientBrutalErr, "failed to enable TCP Brutal at client"))
		}
	}
	return nil
}

func (c *Client) Reset() {
	c.access.Lock()
	defer c.access.Unlock()
	for _, session := range c.connections.Array() {
		if w, ok := session.(*sessionWrapper); ok && w.timer != nil {
			w.timer.Stop()
		}
		go session.Close()
	}
	c.connections.Init()
}

func (c *Client) Close() error {
	c.Reset()
	return nil
}

func (c *Client) maybeCloseSession(s *sessionWrapper, retries int) {
	if s == nil || s.IsClosed() {
		return
	}
	if !s.isRetired(time.Now()) || s.activeStreams.Load() > 0 {
		return
	}
	if s.NumStreams() > 0 {
		if retries < 10 {
			time.AfterFunc(50*time.Millisecond, func() {
				c.maybeCloseSession(s, retries+1)
			})
			return
		}
		if c.logger != nil {
			c.logger.Debug("sing-mux: NumStreams did not reach 0 within drain timeout, force closing retired session")
		}
	}
	c.access.Lock()
	defer c.access.Unlock()
	if s.IsClosed() || !s.isRetired(time.Now()) || s.activeStreams.Load() > 0 {
		return
	}
	if c.logger != nil {
		c.logger.Debug("sing-mux: closing retired session, NumStreams=0")
	}
	_ = s.Close()
	for element := c.connections.Front(); element != nil; element = element.Next() {
		if element.Value == s {
			c.connections.Remove(element)
			break
		}
	}
}

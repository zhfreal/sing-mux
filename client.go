package mux

import (
	"context"
	"math"
	"net"
	"sync"
	"time"

	"github.com/metacubex/sing/common/bufio"
	E "github.com/metacubex/sing/common/exceptions"
	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/x/list"
)

const (
	DefaultTCPTimeout                 = 500 * time.Millisecond
	DefaultMinStreams                 = 8
	DefaultMaxDrainRetries            = 10
	MaxDrainRetries                   = DefaultMaxDrainRetries
	DefaultDrainRetryInterval         = 50 * time.Millisecond
	DrainRetryInterval                = DefaultDrainRetryInterval
	DefaultStreamOpenRetries          = 2
	DefaultMaxStreamSwapRetries       = 2
	MaxStreamSwapRetries              = DefaultMaxStreamSwapRetries
	DefaultBrutalCwndGain             = BrutalDefaultGain
	DefaultStreamSwapDialTimeout      = 1 * time.Second
	StreamSwapDialTimeout             = DefaultStreamSwapDialTimeout
	DefaultMaxHandshakeBufferCapacity = 4096
	MaxHandshakeBufferCapacity        = DefaultMaxHandshakeBufferCapacity
	InitialDrainRetryCount            = 0
	DefaultUnlimitedRequests          = math.MaxInt32
	DefaultUnlimitedReuseTimes        = -1
)

type Client struct {
	dialer                     N.Dialer
	logger                     logger.Logger
	protocol                   byte
	maxConnections             int
	minStreams                 int
	maxStreams                 int
	padding                    bool
	tcpTimeout                 time.Duration
	maxDrainRetries            int
	drainRetryInterval         time.Duration
	maxStreamSwapRetries       int
	streamSwapDialTimeout      time.Duration
	maxHandshakeBufferCapacity int
	streamOpenRetries          int
	h2MuxIdleTimeout           time.Duration
	access                     sync.Mutex
	closed                     bool
	connections                list.List[abstractSession]
	brutal                     BrutalOptions
	cMaxReuseTimes             Range
	hMaxRequestTimes           Range
	hMaxReusableSecs           Range
}

type Options struct {
	Dialer                     N.Dialer
	Logger                     logger.Logger
	Protocol                   string
	MaxConnections             int
	MinStreams                 int
	MaxStreams                 int
	Padding                    bool
	TCPTimeout                 time.Duration
	MaxDrainRetries            int
	DrainRetryInterval         time.Duration
	MaxStreamSwapRetries       int
	StreamSwapDialTimeout      time.Duration
	MaxHandshakeBufferCapacity int
	StreamOpenRetries          int
	H2MuxIdleTimeout           time.Duration
	Brutal                     BrutalOptions
	CMaxReuseTimes             any
	HMaxRequestTimes           any
	HMaxReusableSecs           any
}

type BrutalOptions struct {
	Enabled            bool
	SendBPS            uint64
	ReceiveBPS         uint64
	Version            uint16
	CwndGain           uint32
	GroupID            uint64
	AllowClientGroupID bool
}

func NewClient(options Options) (*Client, error) {
	client := &Client{
		dialer:                     options.Dialer,
		logger:                     options.Logger,
		maxConnections:             options.MaxConnections,
		minStreams:                 options.MinStreams,
		maxStreams:                 options.MaxStreams,
		padding:                    options.Padding,
		tcpTimeout:                 options.TCPTimeout,
		maxDrainRetries:            options.MaxDrainRetries,
		drainRetryInterval:         options.DrainRetryInterval,
		maxStreamSwapRetries:       options.MaxStreamSwapRetries,
		streamSwapDialTimeout:      options.StreamSwapDialTimeout,
		maxHandshakeBufferCapacity: options.MaxHandshakeBufferCapacity,
		streamOpenRetries:          options.StreamOpenRetries,
		h2MuxIdleTimeout:           options.H2MuxIdleTimeout,
		brutal:                     options.Brutal,
		cMaxReuseTimes:             parseRange(options.CMaxReuseTimes),
		hMaxRequestTimes:           parseRange(options.HMaxRequestTimes),
		hMaxReusableSecs:           parseRange(options.HMaxReusableSecs),
	}
	if client.dialer == nil {
		client.dialer = N.SystemDialer
	}
	if client.maxStreams == 0 && client.maxConnections == 0 {
		client.minStreams = DefaultMinStreams
	}
	if client.tcpTimeout == 0 {
		client.tcpTimeout = DefaultTCPTimeout
	}
	if client.maxDrainRetries <= 0 {
		client.maxDrainRetries = MaxDrainRetries
	}
	if client.drainRetryInterval <= 0 {
		client.drainRetryInterval = DrainRetryInterval
	}
	if client.maxStreamSwapRetries <= 0 {
		client.maxStreamSwapRetries = MaxStreamSwapRetries
	}
	if client.streamSwapDialTimeout <= 0 {
		client.streamSwapDialTimeout = DefaultStreamSwapDialTimeout
	}
	if client.maxHandshakeBufferCapacity <= 0 {
		client.maxHandshakeBufferCapacity = DefaultMaxHandshakeBufferCapacity
	}
	if client.streamOpenRetries <= 0 {
		client.streamOpenRetries = DefaultStreamOpenRetries
	}
	if client.h2MuxIdleTimeout <= 0 {
		client.h2MuxIdleTimeout = DefaultH2MuxIdleTimeout
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
	c.access.Lock()
	if c.closed {
		c.access.Unlock()
		return nil, net.ErrClosed
	}
	c.access.Unlock()
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		stream, err := c.openStream(ctx)
		if err != nil {
			return nil, err
		}
		conn := &clientConn{
			conn:        stream,
			destination: destination,
			client:      c,
		}
		// h2mux and yamux support half-close; expose CloseWrite so callers
		// send END_STREAM / FIN instead of falling back to a full close
		// (which is RST_STREAM on h2mux and drops in-flight data). smux v1
		// has no half-close, so its stream is deliberately left without one.
		if c.protocol == ProtocolH2Mux || c.protocol == ProtocolYAMux {
			return &clientConnWithCloseWrite{clientConn: conn}, nil
		}
		return conn, nil
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
	c.access.Lock()
	if c.closed {
		c.access.Unlock()
		return nil, net.ErrClosed
	}
	c.access.Unlock()
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
	retries := c.streamOpenRetries
	if retries <= 0 {
		retries = DefaultStreamOpenRetries
	}
	for attempts := 0; attempts < retries; attempts++ {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		session, err = c.offer(ctx)
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				return nil, err
			}
			continue
		}
		stream, err = session.Open(c.tcpTimeout)
		if err != nil {
			if w, ok := session.(*sessionWrapper); ok {
				w.activeStreams.Add(-1)
				c.CloseSession(w)
			} else {
				c.CloseSession(session)
			}
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
		Conn:    stream,
		session: session,
		onClose: func() {
			if wrapper != nil {
				wrapper.activeStreams.Add(-1)
				c.maybeCloseSession(wrapper, InitialDrainRetryCount)
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

	if c.closed {
		return nil, net.ErrClosed
	}

	now := time.Now()
	var (
		validSessionCount int
		firstAvailable    abstractSession
		minSession        abstractSession
		minStreams        = math.MaxInt
	)
	for element := c.connections.Front(); element != nil; {
		sess := element.Value
		if sess.IsClosed() {
			sess.Close()
			nextElement := element.Next()
			c.connections.Remove(element)
			element = nextElement
			continue
		}
		if wrapper, ok := sess.(*sessionWrapper); ok && wrapper.isRetired(now) {
			if wrapper.activeStreams.Load() <= 0 && wrapper.NumStreams() <= 0 {
				wrapper.Close()
				nextElement := element.Next()
				c.connections.Remove(element)
				element = nextElement
				continue
			}
			element = element.Next()
			continue
		}
		validSessionCount++
		if sess.CanTakeNewRequest() {
			if c.brutal.Enabled {
				if firstAvailable == nil {
					firstAvailable = sess
				}
			} else {
				ns := sess.NumStreams()
				if ns < minStreams {
					minStreams = ns
					minSession = sess
				}
			}
		}
		element = element.Next()
	}
	if c.brutal.Enabled {
		if firstAvailable != nil {
			if c.maxStreams > 0 && firstAvailable.NumStreams() >= c.maxStreams {
				return c.offerNew(ctx)
			}
			if wrapper, ok := firstAvailable.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return firstAvailable, nil
		}
		return c.offerNew(ctx)
	}
	if minSession == nil {
		return c.offerNew(ctx)
	}
	if minStreams == 0 {
		if wrapper, ok := minSession.(*sessionWrapper); ok {
			wrapper.activeStreams.Add(1)
			wrapper.ConsumeReuse()
		}
		return minSession, nil
	}
	if c.maxConnections > 0 {
		if validSessionCount >= c.maxConnections || minStreams < c.minStreams {
			if wrapper, ok := minSession.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return minSession, nil
		}
	} else {
		if c.maxStreams > 0 && minStreams < c.maxStreams {
			if wrapper, ok := minSession.(*sessionWrapper); ok {
				wrapper.activeStreams.Add(1)
				wrapper.ConsumeReuse()
			}
			return minSession, nil
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
	rawSession, err := newClientSession(conn, c.protocol, c.h2MuxIdleTimeout)
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
	wrapper.leftRequests.Store(DefaultUnlimitedRequests)
	if req := c.hMaxRequestTimes.Rand(); req > 0 {
		wrapper.leftRequests.Store(int32(req))
	}
	wrapper.leftReuseTimes.Store(DefaultUnlimitedReuseTimes)
	if reuse := c.cMaxReuseTimes.Rand(); reuse > 0 {
		wrapper.leftReuseTimes.Store(int32(reuse))
	}
	if secs := c.hMaxReusableSecs.Rand(); secs > 0 {
		wrapper.unreusableAt = now.Add(time.Duration(secs) * time.Second)
		wrapper.timer = time.AfterFunc(time.Duration(secs)*time.Second, func() {
			c.maybeCloseSession(wrapper, InitialDrainRetryCount)
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
	wantV2 := c.brutal.Version == BrutalProtocolVersionV2 || (c.brutal.Version == 0 && c.brutal.GroupID > 0)
	port := BrutalExchangePortV1
	if wantV2 {
		port = BrutalExchangePortV2
	}
	exchangeTimeout := c.tcpTimeout
	if exchangeTimeout <= 0 {
		exchangeTimeout = DefaultTCPTimeout
	}
	deadline := time.Now().Add(exchangeTimeout)
	if ctx != nil {
		if dl, ok := ctx.Deadline(); ok {
			deadline = dl
		}
	}
	_ = stream.SetDeadline(deadline)
	conn := &clientConn{conn: &wrapStream{Conn: stream}, destination: M.Socksaddr{Fqdn: BrutalExchangeDomain, Port: port}}
	defer conn.Close()
	exchangeTimer := time.AfterFunc(time.Until(deadline), func() {
		_ = conn.Close()
	})
	defer exchangeTimer.Stop()
	gain := c.brutal.CwndGain
	if gain == 0 {
		gain = DefaultBrutalCwndGain
	}
	req := BrutalExchangeRequest{
		ReceiveBPS: c.brutal.ReceiveBPS,
		CwndGain:   uint16(gain),
		GroupID:    c.brutal.GroupID,
	}
	err = WriteBrutalRequest(conn, req)
	if err != nil {
		return err
	}
	resp, isV2, err := ReadBrutalResponse(conn, wantV2)
	if err != nil {
		return err
	}
	sendBPS := c.brutal.SendBPS
	if resp.ReceiveBPS < sendBPS {
		sendBPS = resp.ReceiveBPS
	}
	groupID := c.brutal.GroupID
	if isV2 && resp.GroupID != 0 {
		groupID = resp.GroupID
	}
	clientGain := c.brutal.CwndGain
	if clientGain == 0 {
		clientGain = uint32(resp.CwndGain)
	}
	if clientGain == 0 {
		clientGain = DefaultBrutalCwndGain
	}
	clientBrutalErr := SetBrutalOptions(sessionConn, sendBPS, groupID, clientGain)
	if clientBrutalErr != nil {
		if c.logger != nil {
			c.logger.Error(E.Cause(clientBrutalErr, "failed to enable TCP Brutal on client socket"))
		}
	} else if c.logger != nil {
		if isV2 {
			c.logger.Debug("brutal v2 negotiated: sendBPS=", sendBPS, " groupID=", groupID, " cwndGain=", resp.CwndGain)
		} else if wantV2 {
			c.logger.Debug("server does not support brutal v2, cleanly degraded to brutal v1: sendBPS=", sendBPS)
		} else {
			c.logger.Debug("brutal v1 negotiated: sendBPS=", sendBPS)
		}
	}
	return nil
}

func (c *Client) CloseSession(session abstractSession) {
	if session == nil {
		return
	}
	c.access.Lock()
	defer c.access.Unlock()
	for element := c.connections.Front(); element != nil; element = element.Next() {
		if element.Value == session {
			if w, ok := session.(*sessionWrapper); ok && w.timer != nil {
				w.timer.Stop()
			}
			go session.Close()
			c.connections.Remove(element)
			break
		}
	}
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
	c.access.Lock()
	c.closed = true
	c.access.Unlock()
	c.Reset()
	return nil
}

func (c *Client) maybeCloseSession(s *sessionWrapper, retries int) {
	c.access.Lock()
	closed := c.closed
	c.access.Unlock()
	if closed {
		return
	}
	if s == nil || s.IsClosed() {
		return
	}
	if !s.isRetired(time.Now()) || s.activeStreams.Load() > 0 {
		return
	}
	if s.NumStreams() > 0 {
		maxRetries := c.maxDrainRetries
		if maxRetries <= 0 {
			maxRetries = MaxDrainRetries
		}
		interval := c.drainRetryInterval
		if interval <= 0 {
			interval = DrainRetryInterval
		}
		if retries < maxRetries {
			time.AfterFunc(interval, func() {
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

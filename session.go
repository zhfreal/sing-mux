package mux

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	E "github.com/metacubex/sing/common/exceptions"
	"github.com/metacubex/smux"
	"github.com/metacubex/yamux"
)

type abstractSession interface {
	Open(tcpTimeout time.Duration) (net.Conn, error)
	Accept() (net.Conn, error)
	NumStreams() int
	Close() error
	IsClosed() bool
	CanTakeNewRequest() bool
}

type sessionWrapper struct {
	abstractSession
	createdAt      time.Time
	unreusableAt   time.Time
	timer          *time.Timer
	leftRequests   atomic.Int32
	leftReuseTimes atomic.Int32
	activeStreams  atomic.Int32
	retired        atomic.Bool
	closed         atomic.Bool
}

func (s *sessionWrapper) isRetired(now time.Time) bool {
	if s.retired.Load() || s.IsClosed() {
		return true
	}
	if !s.unreusableAt.IsZero() && now.After(s.unreusableAt) {
		s.retired.Store(true)
		return true
	}
	if s.leftRequests.Load() <= 0 || s.leftReuseTimes.Load() == 0 {
		s.retired.Store(true)
		return true
	}
	return false
}

func (s *sessionWrapper) CanTakeNewRequest() bool {
	if s.isRetired(time.Now()) {
		return false
	}
	return s.abstractSession.CanTakeNewRequest()
}

func (s *sessionWrapper) IsClosed() bool {
	return s.closed.Load() || s.abstractSession.IsClosed()
}

func (s *sessionWrapper) ConsumeReuse() {
	if s.leftReuseTimes.Load() > 0 {
		s.leftReuseTimes.Add(-1)
	}
}

func (s *sessionWrapper) ConsumeRequest() {
	for {
		req := s.leftRequests.Load()
		if req <= 0 {
			s.retired.Store(true)
			return
		}
		if s.leftRequests.CompareAndSwap(req, req-1) {
			if req-1 == 0 {
				s.retired.Store(true)
			}
			return
		}
	}
}

func (s *sessionWrapper) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.retired.Store(true)
	if s.timer != nil {
		s.timer.Stop()
	}
	return s.abstractSession.Close()
}

type Range struct {
	Min int
	Max int
}

func (r Range) Rand() int {
	if r.Min == r.Max {
		return r.Min
	}
	if r.Min > r.Max {
		return r.Max
	}
	return r.Min + rand.Intn(r.Max-r.Min+1)
}

func parseRange(v any) Range {
	if v == nil {
		return Range{}
	}
	s := strings.TrimSpace(fmt.Sprint(v))
	if s == "" || s == "0" {
		return Range{}
	}
	parts := strings.Split(s, "-")
	if len(parts) == 1 {
		n, err := strconv.Atoi(parts[0])
		if err != nil || n <= 0 {
			return Range{}
		}
		return Range{Min: n, Max: n}
	} else if len(parts) == 2 {
		minVal, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		maxVal, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err1 != nil || err2 != nil {
			return Range{}
		}
		if minVal < 0 {
			minVal = 0
		}
		if maxVal < 0 {
			maxVal = 0
		}
		if minVal > maxVal {
			minVal, maxVal = maxVal, minVal
		}
		if maxVal == 0 {
			return Range{}
		}
		return Range{Min: minVal, Max: maxVal}
	}
	return Range{}
}

func newClientSession(conn net.Conn, protocol byte) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		session, err := newH2MuxClient(conn)
		if err != nil {
			return nil, err
		}
		return session, nil
	case ProtocolSmux:
		client, err := smux.Client(conn, smuxConfig())
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		client, err := yamux.Client(conn, yaMuxConfig(), nil)
		if err != nil {
			return nil, err
		}
		return &yamuxSession{client}, nil
	default:
		return nil, E.New("unexpected protocol ", protocol)
	}
}

func newServerSession(conn net.Conn, protocol byte) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		return newH2MuxServer(conn), nil
	case ProtocolSmux:
		client, err := smux.Server(conn, smuxConfig())
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		client, err := yamux.Server(conn, yaMuxConfig(), nil)
		if err != nil {
			return nil, err
		}
		return &yamuxSession{client}, nil
	default:
		return nil, E.New("unexpected protocol ", protocol)
	}
}

var _ abstractSession = (*smuxSession)(nil)

type smuxSession struct {
	*smux.Session
}

func (s *smuxSession) Open(tcpTimeout time.Duration) (net.Conn, error) {
	return s.OpenStream()
}

func (s *smuxSession) Accept() (net.Conn, error) {
	return s.AcceptStream()
}

func (s *smuxSession) CanTakeNewRequest() bool {
	return true
}

type yamuxSession struct {
	*yamux.Session
}

func (s *yamuxSession) Open(tcpTimeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), tcpTimeout)
	defer cancel()
	return s.Session.Open(ctx)
}

func (y *yamuxSession) CanTakeNewRequest() bool {
	return true
}

func smuxConfig() *smux.Config {
	config := smux.DefaultConfig()
	config.KeepAliveDisabled = true
	return config
}

func yaMuxConfig() *yamux.Config {
	config := yamux.DefaultConfig()
	config.LogOutput = io.Discard
	//config.StreamCloseTimeout = TCPTimeout
	//config.StreamOpenTimeout = TCPTimeout
	return config
}

package mux

import (
	"context"
	"fmt"
	"io"
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
	return s.closed.Load() || (s.abstractSession != nil && s.abstractSession.IsClosed())
}

func (s *sessionWrapper) ConsumeReuse() {
	for {
		reuse := s.leftReuseTimes.Load()
		if reuse <= 0 {
			return
		}
		if s.leftReuseTimes.CompareAndSwap(reuse, reuse-1) {
			if reuse-1 == 0 {
				s.retired.Store(true)
			}
			return
		}
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

const (
	splitMix64Gamma  = uint64(0x9e3779b97f4a7c15)
	splitMix64Mix1   = uint64(0xbf58476d1ce4e5b9)
	splitMix64Mix2   = uint64(0x94d049bb133111eb)
	splitMix64Shift1 = 30
	splitMix64Shift2 = 27
	splitMix64Shift3 = 31
	splitMix64Shift4 = 33
)

var fastRandState atomic.Uint64

func init() {
	fastRandState.Store(uint64(time.Now().UnixNano()) | 1)
}

func fastRandIntn(n int) int {
	if n <= 1 {
		return 0
	}
	z := fastRandState.Add(splitMix64Gamma)
	z = (z ^ (z >> splitMix64Shift1)) * splitMix64Mix1
	z = (z ^ (z >> splitMix64Shift2)) * splitMix64Mix2
	z ^= z >> splitMix64Shift3
	return int((z >> splitMix64Shift4) % uint64(n))
}

func (r Range) Rand() int {
	if r.Min == r.Max {
		return r.Min
	}
	if r.Min > r.Max {
		return r.Max
	}
	return r.Min + fastRandIntn(r.Max-r.Min+1)
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

const (
	DefaultSmuxMaxReceiveBuffer     = 16 * 1024 * 1024 // 16 MiB
	DefaultSmuxMaxStreamBuffer      = 2 * 1024 * 1024  // 2 MiB
	DefaultYamuxMaxStreamWindowSize = 2 * 1024 * 1024  // 2 MiB
	MinYamuxStreamWindowSize        = 256 * 1024       // 256 KiB yamux protocol floor
)

// ParseBytes parses an integer byte count or human-readable size string
// (e.g. 16777216, "16MB", "16 MiB", "2M", "64KB", "64k", "1G").
// It handles numeric types (including JSON unmarshaled float64), stringers,
// pointers, and returns 0 for empty, non-positive, or unparseable input.
func ParseBytes(v any) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		if val > 0 {
			return val
		}
		return 0
	case int32:
		if val > 0 {
			return int(val)
		}
		return 0
	case int64:
		if val > 0 {
			return int(val)
		}
		return 0
	case uint:
		return int(val)
	case uint32:
		return int(val)
	case uint64:
		return int(val)
	case float64:
		if val > 0 {
			return int(val)
		}
		return 0
	case float32:
		if val > 0 {
			return int(val)
		}
		return 0
	case *int:
		if val != nil {
			return ParseBytes(*val)
		}
		return 0
	case *int64:
		if val != nil {
			return ParseBytes(*val)
		}
		return 0
	case *string:
		if val != nil {
			return ParseBytes(*val)
		}
		return 0
	case string:
		s := strings.TrimSpace(strings.ToUpper(val))
		if s == "" || s == "0" {
			return 0
		}
		s = strings.ReplaceAll(s, " ", "")

		multiplier := 1
		if strings.HasSuffix(s, "GIB") || strings.HasSuffix(s, "GB") {
			multiplier = 1024 * 1024 * 1024
			if strings.HasSuffix(s, "GIB") {
				s = strings.TrimSuffix(s, "GIB")
			} else {
				s = strings.TrimSuffix(s, "GB")
			}
		} else if strings.HasSuffix(s, "G") {
			multiplier = 1024 * 1024 * 1024
			s = strings.TrimSuffix(s, "G")
		} else if strings.HasSuffix(s, "MIB") || strings.HasSuffix(s, "MB") {
			multiplier = 1024 * 1024
			if strings.HasSuffix(s, "MIB") {
				s = strings.TrimSuffix(s, "MIB")
			} else {
				s = strings.TrimSuffix(s, "MB")
			}
		} else if strings.HasSuffix(s, "M") {
			multiplier = 1024 * 1024
			s = strings.TrimSuffix(s, "M")
		} else if strings.HasSuffix(s, "KIB") || strings.HasSuffix(s, "KB") {
			multiplier = 1024
			if strings.HasSuffix(s, "KIB") {
				s = strings.TrimSuffix(s, "KIB")
			} else {
				s = strings.TrimSuffix(s, "KB")
			}
		} else if strings.HasSuffix(s, "K") {
			multiplier = 1024
			s = strings.TrimSuffix(s, "K")
		} else if strings.HasSuffix(s, "B") {
			s = strings.TrimSuffix(s, "B")
		}

		num, err := strconv.ParseFloat(s, 64)
		if err != nil || num <= 0 {
			return 0
		}
		return int(num * float64(multiplier))
	case fmt.Stringer:
		return ParseBytes(val.String())
	default:
		return 0
	}
}

func newClientSession(conn net.Conn, protocol byte, h2MuxIdleTimeout time.Duration, maxReceiveBuffer, maxStreamBuffer int, maxStreamWindowSize uint32) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		session, err := newH2MuxClient(conn, h2MuxIdleTimeout)
		if err != nil {
			return nil, err
		}
		return session, nil
	case ProtocolSmux:
		client, err := smux.Client(conn, smuxConfig(maxReceiveBuffer, maxStreamBuffer))
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		client, err := yamux.Client(conn, yaMuxConfig(maxStreamWindowSize), nil)
		if err != nil {
			return nil, err
		}
		return &yamuxSession{client}, nil
	default:
		return nil, E.New("unexpected protocol ", protocol)
	}
}

func newServerSession(conn net.Conn, protocol byte, h2MuxIdleTimeout time.Duration, maxReceiveBuffer, maxStreamBuffer int, maxStreamWindowSize uint32) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		return newH2MuxServer(conn, h2MuxIdleTimeout), nil
	case ProtocolSmux:
		client, err := smux.Server(conn, smuxConfig(maxReceiveBuffer, maxStreamBuffer))
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		client, err := yamux.Server(conn, yaMuxConfig(maxStreamWindowSize), nil)
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

type yamuxWrapStream struct {
	*yamux.Stream
}

func (w *yamuxWrapStream) Read(p []byte) (n int, err error) {
	n, err = w.Stream.Read(p)
	return n, wrapError(err)
}

func (w *yamuxWrapStream) Write(p []byte) (n int, err error) {
	n, err = w.Stream.Write(p)
	return n, wrapError(err)
}

// CloseWrite half-closes the stream: it sends FIN and keeps the read side
// open. metacubex/yamux (unlike hashicorp/yamux) implements Close() as
// CloseRead()+CloseWrite(), so calling Close() here would discard the
// peer's response.
func (w *yamuxWrapStream) CloseWrite() error {
	return w.Stream.CloseWrite()
}

func (w *yamuxWrapStream) Upstream() any {
	return w.Stream
}

func smuxConfig(maxReceiveBuffer, maxStreamBuffer int) *smux.Config {
	config := smux.DefaultConfig()
	config.KeepAliveDisabled = true
	if maxReceiveBuffer > 0 {
		config.MaxReceiveBuffer = maxReceiveBuffer
	} else {
		config.MaxReceiveBuffer = DefaultSmuxMaxReceiveBuffer
	}
	if maxStreamBuffer > 0 {
		config.MaxStreamBuffer = maxStreamBuffer
	} else {
		config.MaxStreamBuffer = DefaultSmuxMaxStreamBuffer
	}
	// smux.VerifyConfig requires MaxStreamBuffer <= MaxReceiveBuffer.
	if config.MaxStreamBuffer > config.MaxReceiveBuffer {
		config.MaxReceiveBuffer = config.MaxStreamBuffer
	}
	return config
}

func yaMuxConfig(maxStreamWindowSize uint32) *yamux.Config {
	config := yamux.DefaultConfig()
	config.LogOutput = io.Discard
	if maxStreamWindowSize > 0 {
		if maxStreamWindowSize < MinYamuxStreamWindowSize {
			maxStreamWindowSize = MinYamuxStreamWindowSize
		}
		config.MaxStreamWindowSize = maxStreamWindowSize
	} else {
		config.MaxStreamWindowSize = DefaultYamuxMaxStreamWindowSize
	}
	if config.MaxStreamWindowSize < config.InitialStreamWindowSize {
		config.InitialStreamWindowSize = config.MaxStreamWindowSize
	}
	return config
}

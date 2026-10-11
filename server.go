package mux

import (
	"context"
	"net"

	"github.com/metacubex/sing/common/bufio"
	"github.com/metacubex/sing/common/debug"
	E "github.com/metacubex/sing/common/exceptions"
	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/task"
	"github.com/metacubex/yamux"
)

type ServiceHandler interface {
	N.TCPConnectionHandler
	N.UDPConnectionHandler
}

type Service struct {
	newStreamContext func(context.Context, net.Conn) context.Context
	logger           logger.ContextLogger
	handler          ServiceHandler
	padding          bool
	brutal           BrutalOptions
}

type ServiceOptions struct {
	NewStreamContext func(context.Context, net.Conn) context.Context
	Logger           logger.ContextLogger
	Handler          ServiceHandler
	Padding          bool
	Brutal           BrutalOptions
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Brutal.Enabled && !BrutalAvailable && !debug.Enabled {
		return nil, E.New("TCP Brutal is only supported on Linux")
	}
	return &Service{
		newStreamContext: options.NewStreamContext,
		logger:           options.Logger,
		handler:          options.Handler,
		padding:          options.Padding,
		brutal:           options.Brutal,
	}, nil
}

func (s *Service) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	request, err := ReadRequest(conn)
	if err != nil {
		return err
	}
	if request.Padding {
		conn = newPaddingConn(conn)
	} else if s.padding {
		return E.New("non-padded connection rejected")
	}
	session, err := newServerSession(conn, request.Protocol)
	if err != nil {
		return err
	}
	var group task.Group
	group.Append0(func(_ context.Context) error {
		for {
			stream, aErr := session.Accept()
			if aErr != nil {
				return aErr
			}
			streamCtx := s.newStreamContext(ctx, stream)
			go func() {
				hErr := s.newConnection(streamCtx, conn, stream, metadata)
				if hErr != nil && s.logger != nil {
					s.logger.ErrorContext(streamCtx, E.Cause(hErr, "handle connection"))
				}
			}()
		}
	})
	group.Cleanup(func() {
		session.Close()
	})
	return group.Run(ctx)
}

func (s *Service) newConnection(ctx context.Context, sessionConn net.Conn, stream net.Conn, metadata M.Metadata) error {
	if yamuxStream, ok := stream.(*yamux.Stream); ok {
		stream = &yamuxWrapStream{yamuxStream}
	} else {
		stream = &wrapStream{Conn: stream}
	}
	request, err := ReadStreamRequest(stream)
	if err != nil {
		return E.Cause(err, "read multiplex stream request")
	}
	metadata.Destination = request.Destination
	if request.Network == N.NetworkTCP {
		conn := &serverConn{ExtendedConn: bufio.NewExtendedConn(stream)}
		if request.Destination.Fqdn == BrutalExchangeDomain {
			defer stream.Close()
			isV2 := request.Destination.Port == BrutalExchangePortV2
			req, err := ReadBrutalRequest(conn)
			if err != nil {
				return E.Cause(err, "read brutal request")
			}
			if !s.brutal.Enabled {
				err = WriteBrutalResponse(conn, BrutalExchangeResponse{}, false, "brutal is not enabled by the server")
				if err != nil {
					return E.Cause(err, "write brutal response")
				}
				return nil
			}
			clientReceiveBPS := req.ReceiveBPS
			sendBPS := s.brutal.SendBPS
			if clientReceiveBPS < sendBPS {
				sendBPS = clientReceiveBPS
			}
			var groupID uint64
			if s.brutal.GroupID > 0 {
				groupID = s.brutal.GroupID
			} else if s.brutal.AllowClientGroupID && isV2 {
				userTenant := ""
				if metadata.Source.Addr.IsValid() {
					userTenant = metadata.Source.Addr.String()
				} else if metadata.Source.Fqdn != "" {
					userTenant = metadata.Source.Fqdn
				}
				if userTenant == "" && sessionConn != nil && sessionConn.RemoteAddr() != nil {
					if host, _, err := net.SplitHostPort(sessionConn.RemoteAddr().String()); err == nil {
						userTenant = host
					} else {
						userTenant = sessionConn.RemoteAddr().String()
					}
				}
				if userTenant != "" || req.GroupID > 0 {
					groupID = deriveTenantGroupID(userTenant, req.GroupID)
				}
			}
			gain := uint32(req.CwndGain)
			if gain == 0 {
				gain = 20
			}
			err = SetBrutalOptions(sessionConn, sendBPS, groupID, gain)
			if err != nil {
				// ignore error in test
				if !debug.Enabled {
					err = WriteBrutalResponse(conn, BrutalExchangeResponse{}, false, E.Cause(err, "enable TCP Brutal").Error())
					if err != nil {
						return E.Cause(err, "write brutal response")
					}
					return nil
				}
				if s.logger != nil {
					s.logger.ErrorContext(ctx, E.Cause(err, "failed to enable TCP Brutal on server socket"))
				}
			} else if s.logger != nil {
				if isV2 {
					s.logger.DebugContext(ctx, "inbound brutal v2 negotiated: sendBPS=", sendBPS, " groupID=", groupID, " gain=", gain)
				} else {
					s.logger.DebugContext(ctx, "inbound brutal v1 negotiated: sendBPS=", sendBPS)
				}
			}
			err = WriteBrutalResponse(conn, BrutalExchangeResponse{
				ReceiveBPS: s.brutal.ReceiveBPS,
				GroupID:    groupID,
				CwndGain:   gain,
				IsV2:       isV2,
			}, true, "")
			if err != nil {
				return E.Cause(err, "write brutal response")
			}
			return nil
		}
		if s.logger != nil {
			s.logger.InfoContext(ctx, "inbound multiplex connection to ", metadata.Destination)
		}
		s.handler.NewConnection(ctx, conn, metadata)
		stream.Close()
	} else {
		var packetConn N.PacketConn
		if !request.PacketAddr {
			if s.logger != nil {
				s.logger.InfoContext(ctx, "inbound multiplex packet connection to ", metadata.Destination)
			}
			packetConn = &serverPacketConn{ExtendedConn: bufio.NewExtendedConn(stream), destination: request.Destination}
		} else {
			if s.logger != nil {
				s.logger.InfoContext(ctx, "inbound multiplex packet connection")
			}
			packetConn = &serverPacketAddrConn{ExtendedConn: bufio.NewExtendedConn(stream)}
		}
		s.handler.NewPacketConnection(ctx, packetConn, metadata)
		stream.Close()
	}
	return nil
}

package mux

import (
	"encoding/binary"
	"io"
	"math/rand"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
	"github.com/metacubex/sing/common/rw"
)

const (
	ProtocolSmux = iota
	ProtocolYAMux
	ProtocolH2Mux
)

const (
	Version0 = iota
	Version1
)

var Destination = M.Socksaddr{
	Fqdn: "sp.mux.sing-box.arpa",
	Port: 444,
}

type Request struct {
	Version  byte
	Protocol byte
	Padding  bool
}

func readUint16(reader io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(reader, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

func writeUint16(writer io.Writer, v uint16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	_, err := writer.Write(b[:])
	return err
}

func ReadRequest(reader io.Reader) (*Request, error) {
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	version := header[0]
	protocol := header[1]
	if version < Version0 || version > Version1 {
		return nil, E.New("unsupported version: ", version)
	}
	var paddingEnabled bool
	if version == Version1 {
		var padByte [1]byte
		if _, err := io.ReadFull(reader, padByte[:]); err != nil {
			return nil, err
		}
		paddingEnabled = padByte[0] != 0
		if paddingEnabled {
			paddingLen, err := readUint16(reader)
			if err != nil {
				return nil, err
			}
			err = rw.SkipN(reader, int(paddingLen))
			if err != nil {
				return nil, err
			}
		}
	}
	return &Request{Version: version, Protocol: protocol, Padding: paddingEnabled}, nil
}

func EncodeRequest(request Request, payload []byte) *buf.Buffer {
	var requestLen int
	requestLen += 2
	var paddingLen uint16
	if request.Version == Version1 {
		requestLen += 1
		if request.Padding {
			requestLen += 2
			paddingLen = uint16(256 + rand.Intn(512))
			requestLen += int(paddingLen)
		}
	}
	buffer := buf.NewSize(requestLen + len(payload))
	common.Must(
		buffer.WriteByte(request.Version),
		buffer.WriteByte(request.Protocol),
	)
	if request.Version == Version1 {
		var padByte byte
		if request.Padding {
			padByte = 1
		}
		common.Must(buffer.WriteByte(padByte))
		if request.Padding {
			binary.BigEndian.PutUint16(buffer.Extend(2), paddingLen)
			buffer.Extend(int(paddingLen))
		}
	}
	common.Must1(buffer.Write(payload))
	return buffer
}

const (
	flagUDP       = 1
	flagAddr      = 2
	statusSuccess = 0
	statusError   = 1
)

type StreamRequest struct {
	Network     string
	Destination M.Socksaddr
	PacketAddr  bool
}

func ReadStreamRequest(reader io.Reader) (*StreamRequest, error) {
	flags, err := readUint16(reader)
	if err != nil {
		return nil, err
	}
	destination, err := M.SocksaddrSerializer.ReadAddrPort(reader)
	if err != nil {
		return nil, err
	}
	var network string
	var udpAddr bool
	if flags&flagUDP == 0 {
		network = N.NetworkTCP
	} else {
		network = N.NetworkUDP
		udpAddr = flags&flagAddr != 0
	}
	return &StreamRequest{network, destination, udpAddr}, nil
}

func streamRequestLen(request StreamRequest) int {
	var rLen int
	rLen += 1 // version
	rLen += 2 // flags
	rLen += M.SocksaddrSerializer.AddrPortLen(request.Destination)
	return rLen
}

func EncodeStreamRequest(request StreamRequest, buffer *buf.Buffer) error {
	destination := request.Destination
	var flags uint16
	if request.Network == N.NetworkUDP {
		flags |= flagUDP
	}
	if request.PacketAddr {
		flags |= flagAddr
		if !destination.IsValid() {
			destination = Destination
		}
	}
	binary.BigEndian.PutUint16(buffer.Extend(2), flags)
	return M.SocksaddrSerializer.WriteAddrPort(buffer, destination)
}

type StreamResponse struct {
	Status  uint8
	Message string
}

func ReadStreamResponse(reader io.Reader) (*StreamResponse, error) {
	var statusBuf [1]byte
	if _, err := io.ReadFull(reader, statusBuf[:]); err != nil {
		return nil, err
	}
	response := StreamResponse{Status: statusBuf[0]}
	if response.Status == statusError {
		var err error
		response.Message, err = rw.ReadVString(reader)
		if err != nil {
			return nil, err
		}
	}
	return &response, nil
}

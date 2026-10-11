package mux

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/buf"
	E "github.com/metacubex/sing/common/exceptions"
	"github.com/metacubex/sing/common/rw"
)

const (
	BrutalExchangeDomain = "_BrutalBwExchange"
	BrutalExchangePortV2 = uint16(23302)      // Matches TCP_BRUTAL_VERSION sockopt to disambiguate v2 requests
	BrutalMagicV2        = uint32(0x42525502) // "BRU\x02" in BigEndian
	BrutalMinSpeedBPS    = 65536              // >= kernel MIN_PACING_RATE (62500 B/s)
	BrutalMaxSpeedBPS    = 125000000000       // kernel MAX_PACING_RATE (1 Tbps)
	BrutalMinCwndGain    = 5                  // kernel MIN_CWND_GAIN (0.5x)
	BrutalMaxCwndGain    = 80                 // kernel MAX_CWND_GAIN (8.0x)
	BrutalDefaultGain    = 20                 // kernel INIT_CWND_GAIN (2.0x)
)

type BrutalExchangeRequest struct {
	ReceiveBPS uint64 // 8 bytes (v1 compatible header)
	Magic      uint32 // 4 bytes: BrutalMagicV2
	Version    uint16 // 2 bytes: 2
	CwndGain   uint16 // 2 bytes: tenths (20 = 2.0x)
	GroupID    uint64 // 8 bytes: group ID
}

type BrutalExchangeResponse struct {
	ReceiveBPS uint64
	CwndGain   uint32
	GroupID    uint64
	IsV2       bool
}

func WriteBrutalRequest(writer io.Writer, req BrutalExchangeRequest) error {
	// Zero-unread-byte invariant: client transmits strictly 8 bytes (rate),
	// relying on destination port 23302 in StreamRequest to declare v2 capability.
	// Eliminates unread byte buffers and prevents legacy v1 servers from aborting with RST.
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], req.ReceiveBPS)
	_, err := writer.Write(b[:])
	return err
}

func ReadBrutalRequest(reader io.Reader) (BrutalExchangeRequest, error) {
	var b [8]byte
	if _, err := io.ReadFull(reader, b[:]); err != nil {
		return BrutalExchangeRequest{}, err
	}
	return BrutalExchangeRequest{
		ReceiveBPS: binary.BigEndian.Uint64(b[:]),
		CwndGain:   20,
	}, nil
}

func WriteBrutalResponse(writer io.Writer, resp BrutalExchangeResponse, ok bool, message string) error {
	if ok {
		if resp.IsV2 {
			var b [25]byte // 1 byte bool + 8 byte rate + 4 byte magic + 8 byte groupID + 4 byte cwndGain
			b[0] = 1
			binary.BigEndian.PutUint64(b[1:9], resp.ReceiveBPS)
			binary.BigEndian.PutUint32(b[9:13], BrutalMagicV2)
			binary.BigEndian.PutUint64(b[13:21], resp.GroupID)
			binary.BigEndian.PutUint32(b[21:25], resp.CwndGain)
			_, err := writer.Write(b[:])
			return err
		}
		var b [9]byte // 1 byte bool + 8 byte rate
		b[0] = 1
		binary.BigEndian.PutUint64(b[1:9], resp.ReceiveBPS)
		_, err := writer.Write(b[:])
		return err
	}
	// Error path (infrequent): allocate buffer for variable-length message
	buffer := buf.New()
	defer buffer.Release()
	buffer.WriteByte(0)
	if err := rw.WriteVString(buffer, message); err != nil {
		return err
	}
	return common.Error(writer.Write(buffer.Bytes()))
}

func ReadBrutalResponse(reader io.Reader, wantV2 bool) (resp BrutalExchangeResponse, isV2 bool, err error) {
	var head [9]byte
	if _, err = io.ReadFull(reader, head[:]); err != nil {
		return resp, false, err
	}
	if head[0] == 0 {
		msg, rErr := rw.ReadVString(reader)
		if rErr != nil {
			return resp, false, rErr
		}
		return resp, false, E.New("remote brutal error: ", msg)
	}
	resp.ReceiveBPS = binary.BigEndian.Uint64(head[1:9])
	if wantV2 {
		var v2Buf [16]byte
		// Use io.ReadFull to detect v2 extension.
		// If connected to legacy v1 server, server wrote only 9 bytes and closed stream.
		// Clean EOF / unexpected EOF on 0 bytes indicates legacy v1 server without RST.
		n, rErr := io.ReadFull(reader, v2Buf[:])
		if rErr == nil && n == 16 {
			magic := binary.BigEndian.Uint32(v2Buf[0:4])
			if magic == BrutalMagicV2 {
				resp.GroupID = binary.BigEndian.Uint64(v2Buf[4:12])
				resp.CwndGain = binary.BigEndian.Uint32(v2Buf[12:16])
				resp.IsV2 = true
				return resp, true, nil
			}
			// Non-matching magic with complete buffer indicates protocol corruption, not legacy EOF
			return resp, false, E.New("invalid brutal v2 extension magic: ", magic)
		}
		// Clean fallback to v1 ONLY on clean stream closure (io.EOF or io.ErrUnexpectedEOF with strictly 0 bytes read)
		if rErr != nil {
			if (errors.Is(rErr, io.EOF) || errors.Is(rErr, io.ErrUnexpectedEOF)) && n == 0 {
				return resp, false, nil
			}
			return resp, false, E.Cause(rErr, "read brutal v2 extension")
		}
	}
	return resp, false, nil
}

// deriveTenantGroupID cryptographically partitions client group IDs by tenant UUID with zero heap allocations.
func deriveTenantGroupID(userUUID string, clientGroupID uint64) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(userUUID); i++ {
		h ^= uint64(userUUID[i])
		h *= prime64
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], clientGroupID)
	for _, v := range b {
		h ^= uint64(v)
		h *= prime64
	}
	if h == 0 {
		return 1
	}
	return h
}

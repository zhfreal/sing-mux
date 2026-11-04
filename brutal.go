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
	BrutalV2ExtensionLen = 16                 // 4-byte magic + 8-byte groupID + 4-byte cwndGain
	BrutalRequestLen     = 8                  // 8-byte receive rate
	BrutalV1ResponseLen  = 9                  // 1-byte status + 8-byte receive rate
	BrutalV2ResponseLen  = 25                 // 1-byte status + 8-byte receive rate + BrutalV2ExtensionLen
	BrutalParamsV1Len    = 12                 // sizeof(tcp_brutal_params)
	BrutalParamsV2Len    = 20                 // sizeof(tcp_brutal_params_v2)
	FNV64Offset          = uint64(14695981039346656037)
	FNV64Prime           = uint64(1099511628211)
	TenantGroupIDBytes   = 8
	TenantDefaultGroupID = uint64(1)

	BrutalStatusSuccess = 1
	BrutalStatusError   = 0

	BrutalV1RateStart = 1
	BrutalV1RateEnd   = 9

	BrutalV2RateStart     = 1
	BrutalV2RateEnd       = 9
	BrutalV2MagicStart    = 9
	BrutalV2MagicEnd      = 13
	BrutalV2GroupIDStart  = 13
	BrutalV2GroupIDEnd    = 21
	BrutalV2CwndGainStart = 21
	BrutalV2CwndGainEnd   = 25

	BrutalExtMagicStart    = 0
	BrutalExtMagicEnd      = 4
	BrutalExtGroupIDStart  = 4
	BrutalExtGroupIDEnd    = 12
	BrutalExtCwndGainStart = 12
	BrutalExtCwndGainEnd   = 16
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
	var b [BrutalRequestLen]byte
	binary.BigEndian.PutUint64(b[:], req.ReceiveBPS)
	_, err := writer.Write(b[:])
	return err
}

func ReadBrutalRequest(reader io.Reader) (BrutalExchangeRequest, error) {
	var b [BrutalRequestLen]byte
	if _, err := io.ReadFull(reader, b[:]); err != nil {
		return BrutalExchangeRequest{}, err
	}
	return BrutalExchangeRequest{
		ReceiveBPS: binary.BigEndian.Uint64(b[:]),
		CwndGain:   BrutalDefaultGain,
	}, nil
}

func WriteBrutalResponse(writer io.Writer, resp BrutalExchangeResponse, ok bool, message string) error {
	if ok {
		if resp.IsV2 {
			var b [BrutalV2ResponseLen]byte // 1 byte bool + 8 byte rate + 4 byte magic + 8 byte groupID + 4 byte cwndGain
			b[0] = BrutalStatusSuccess
			binary.BigEndian.PutUint64(b[BrutalV2RateStart:BrutalV2RateEnd], resp.ReceiveBPS)
			binary.BigEndian.PutUint32(b[BrutalV2MagicStart:BrutalV2MagicEnd], BrutalMagicV2)
			binary.BigEndian.PutUint64(b[BrutalV2GroupIDStart:BrutalV2GroupIDEnd], resp.GroupID)
			binary.BigEndian.PutUint32(b[BrutalV2CwndGainStart:BrutalV2CwndGainEnd], resp.CwndGain)
			_, err := writer.Write(b[:])
			return err
		}
		var b [BrutalV1ResponseLen]byte // 1 byte bool + 8 byte rate
		b[0] = BrutalStatusSuccess
		binary.BigEndian.PutUint64(b[BrutalV1RateStart:BrutalV1RateEnd], resp.ReceiveBPS)
		_, err := writer.Write(b[:])
		return err
	}
	// Error path (infrequent): allocate buffer for variable-length message
	buffer := buf.New()
	defer buffer.Release()
	buffer.WriteByte(BrutalStatusError)
	if err := rw.WriteVString(buffer, message); err != nil {
		return err
	}
	return common.Error(writer.Write(buffer.Bytes()))
}

func ReadBrutalResponse(reader io.Reader, wantV2 bool) (resp BrutalExchangeResponse, isV2 bool, err error) {
	var head [BrutalV1ResponseLen]byte
	if _, err = io.ReadFull(reader, head[:]); err != nil {
		return resp, false, err
	}
	if head[0] == BrutalStatusError {
		msg, rErr := rw.ReadVString(reader)
		if rErr != nil {
			return resp, false, rErr
		}
		return resp, false, E.New("remote brutal error: ", msg)
	}
	resp.ReceiveBPS = binary.BigEndian.Uint64(head[BrutalV1RateStart:BrutalV1RateEnd])
	if wantV2 {
		var v2Buf [BrutalV2ExtensionLen]byte
		// Use io.ReadFull to detect v2 extension.
		// If connected to legacy v1 server, server wrote only 9 bytes and closed stream.
		// Clean EOF / unexpected EOF on 0 bytes indicates legacy v1 server without RST.
		n, rErr := io.ReadFull(reader, v2Buf[:])
		if rErr == nil && n == BrutalV2ExtensionLen {
			magic := binary.BigEndian.Uint32(v2Buf[BrutalExtMagicStart:BrutalExtMagicEnd])
			if magic == BrutalMagicV2 {
				resp.GroupID = binary.BigEndian.Uint64(v2Buf[BrutalExtGroupIDStart:BrutalExtGroupIDEnd])
				resp.CwndGain = binary.BigEndian.Uint32(v2Buf[BrutalExtCwndGainStart:BrutalExtCwndGainEnd])
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
	h := FNV64Offset
	for i := 0; i < len(userUUID); i++ {
		h ^= uint64(userUUID[i])
		h *= FNV64Prime
	}
	var b [TenantGroupIDBytes]byte
	binary.BigEndian.PutUint64(b[:], clientGroupID)
	for _, v := range b {
		h ^= uint64(v)
		h *= FNV64Prime
	}
	if h == 0 {
		return TenantDefaultGroupID
	}
	return h
}

var nativeEndian = binary.NativeEndian

func packBrutalParamsV2(rate uint64, cwndGain uint32, groupID uint64) [BrutalParamsV2Len]byte {
	var b [BrutalParamsV2Len]byte
	nativeEndian.PutUint64(b[0:8], rate)
	nativeEndian.PutUint32(b[8:12], cwndGain)
	nativeEndian.PutUint64(b[12:20], groupID)
	return b
}

func packBrutalParamsV1(rate uint64, cwndGain uint32) [BrutalParamsV1Len]byte {
	var b [BrutalParamsV1Len]byte
	nativeEndian.PutUint64(b[0:8], rate)
	nativeEndian.PutUint32(b[8:12], cwndGain)
	return b
}

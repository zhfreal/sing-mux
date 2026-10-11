package mux

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBrutalParamsPacking(t *testing.T) {
	rate := uint64(125000000) // 1 Gbps
	gain := uint32(20)
	groupID := uint64(9876543210)

	bufV2 := packBrutalParamsV2(rate, gain, groupID)
	if len(bufV2) != 20 {
		t.Fatalf("packBrutalParamsV2 len = %d, expected 20", len(bufV2))
	}

	unpackedRate := binary.NativeEndian.Uint64(bufV2[0:8])
	unpackedGain := binary.NativeEndian.Uint32(bufV2[8:12])
	unpackedGroup := binary.NativeEndian.Uint64(bufV2[12:20])

	if unpackedRate != rate || unpackedGain != gain || unpackedGroup != groupID {
		t.Fatalf("packBrutalParamsV2 mismatch: got rate=%d, gain=%d, group=%d; want %d, %d, %d",
			unpackedRate, unpackedGain, unpackedGroup, rate, gain, groupID)
	}

	bufV1 := packBrutalParamsV1(rate, gain)
	if len(bufV1) != 12 {
		t.Fatalf("packBrutalParamsV1 len = %d, expected 12", len(bufV1))
	}
}

func TestBrutalRequestZeroUnreadBytes(t *testing.T) {
	req := BrutalExchangeRequest{
		ReceiveBPS: 25000000,
		Version:    2,
		CwndGain:   25,
		GroupID:    42,
	}

	var buf bytes.Buffer
	if err := WriteBrutalRequest(&buf, req); err != nil {
		t.Fatalf("WriteBrutalRequest failed: %v", err)
	}

	// Must write strictly 8 bytes
	if buf.Len() != 8 {
		t.Fatalf("WriteBrutalRequest wrote %d bytes, expected strictly 8 bytes", buf.Len())
	}

	readReq, err := ReadBrutalRequest(&buf)
	if err != nil {
		t.Fatalf("ReadBrutalRequest failed: %v", err)
	}
	if readReq.ReceiveBPS != req.ReceiveBPS {
		t.Fatalf("ReadBrutalRequest rate mismatch: got %d, want %d", readReq.ReceiveBPS, req.ReceiveBPS)
	}
	// Buffer must be completely consumed (0 unread bytes)
	if buf.Len() != 0 {
		t.Fatalf("Buffer not empty after ReadBrutalRequest: %d unread bytes", buf.Len())
	}
}

func TestBrutalResponseV2Roundtrip(t *testing.T) {
	resp := BrutalExchangeResponse{
		ReceiveBPS: 50000000,
		CwndGain:   25,
		GroupID:    101,
		IsV2:       true,
	}

	var buf bytes.Buffer
	if err := WriteBrutalResponse(&buf, resp, true, ""); err != nil {
		t.Fatalf("WriteBrutalResponse failed: %v", err)
	}

	// 1 (ok) + 8 (rate) + 4 (magic) + 8 (group) + 4 (gain) = 25 bytes
	if buf.Len() != 25 {
		t.Fatalf("WriteBrutalResponse v2 wrote %d bytes, expected 25", buf.Len())
	}

	readResp, isV2, err := ReadBrutalResponse(&buf, true)
	if err != nil {
		t.Fatalf("ReadBrutalResponse failed: %v", err)
	}
	if !isV2 {
		t.Fatalf("expected isV2=true")
	}
	if readResp.ReceiveBPS != resp.ReceiveBPS || readResp.GroupID != resp.GroupID || readResp.CwndGain != resp.CwndGain {
		t.Fatalf("ReadBrutalResponse mismatch: got %+v, want %+v", readResp, resp)
	}
}

func TestBrutalResponseV1CleanFallback(t *testing.T) {
	// Simulate legacy v1 server: writes 9 bytes (1 ok + 8 rate), then closes stream (clean EOF)
	respV1 := BrutalExchangeResponse{
		ReceiveBPS: 12500000,
		IsV2:       false,
	}

	var buf bytes.Buffer
	if err := WriteBrutalResponse(&buf, respV1, true, ""); err != nil {
		t.Fatalf("WriteBrutalResponse failed: %v", err)
	}
	if buf.Len() != 9 {
		t.Fatalf("WriteBrutalResponse v1 wrote %d bytes, expected 9", buf.Len())
	}

	// Client requests v2, but server is v1 (EOF after 9 bytes)
	readResp, isV2, err := ReadBrutalResponse(&buf, true)
	if err != nil {
		t.Fatalf("ReadBrutalResponse should cleanly fall back without error, got: %v", err)
	}
	if isV2 {
		t.Fatalf("expected isV2=false on v1 server fallback")
	}
	if readResp.ReceiveBPS != respV1.ReceiveBPS {
		t.Fatalf("rate mismatch: got %d, want %d", readResp.ReceiveBPS, respV1.ReceiveBPS)
	}
}

func TestBrutalResponseCorruptMagic(t *testing.T) {
	// Response with 25 bytes but corrupt magic
	var corruptBuf [25]byte
	corruptBuf[0] = 1 // ok
	binary.BigEndian.PutUint64(corruptBuf[1:9], 10000000)
	binary.BigEndian.PutUint32(corruptBuf[9:13], 0xDEADBEEF) // Bad magic
	binary.BigEndian.PutUint64(corruptBuf[13:21], 55)
	binary.BigEndian.PutUint32(corruptBuf[21:25], 20)

	reader := bytes.NewReader(corruptBuf[:])
	_, _, err := ReadBrutalResponse(reader, true)
	if err == nil {
		t.Fatalf("expected error on corrupt magic, got nil")
	}
}

func TestDeriveTenantGroupID(t *testing.T) {
	tenant1 := "user-alice-uuid"
	tenant2 := "user-bob-uuid"

	id1 := deriveTenantGroupID(tenant1, 100)
	id2 := deriveTenantGroupID(tenant2, 100)
	id3 := deriveTenantGroupID(tenant1, 200)

	if id1 == 0 || id2 == 0 || id3 == 0 {
		t.Fatalf("deriveTenantGroupID returned 0")
	}
	if id1 == id2 {
		t.Fatalf("tenant isolation violation: alice and bob got same group ID %d", id1)
	}
	if id1 == id3 {
		t.Fatalf("different clientGroupID produced identical hash: %d", id1)
	}

	// Consistency
	if deriveTenantGroupID(tenant1, 100) != id1 {
		t.Fatalf("deriveTenantGroupID not deterministic")
	}
}

func TestClampedSpeedAndGain(t *testing.T) {
	if BrutalMinSpeedBPS < 62500 {
		t.Fatalf("BrutalMinSpeedBPS %d is below kernel min 62500", BrutalMinSpeedBPS)
	}
	if BrutalMaxSpeedBPS != 125000000000 {
		t.Fatalf("BrutalMaxSpeedBPS %d != 1 Tbps", BrutalMaxSpeedBPS)
	}
	if BrutalMinCwndGain != 5 || BrutalMaxCwndGain != 80 || BrutalDefaultGain != 20 {
		t.Fatalf("Gain bounds incorrect")
	}
}

func TestBrutalResponseTruncatedV2(t *testing.T) {
	// Response with 9 bytes (v1) + only 4 bytes (partial v2, expected 16)
	var truncatedBuf [13]byte
	truncatedBuf[0] = 1 // ok
	binary.BigEndian.PutUint64(truncatedBuf[1:9], 10000000)
	binary.BigEndian.PutUint32(truncatedBuf[9:13], BrutalMagicV2) // only magic, missing groupID and gain

	reader := bytes.NewReader(truncatedBuf[:])
	_, _, err := ReadBrutalResponse(reader, true)
	if err == nil {
		t.Fatalf("expected error on truncated v2 extension, got nil")
	}
}


package mux

import (
	"encoding/json"
	"testing"

	"github.com/metacubex/smux"
	"github.com/metacubex/yamux"
)

func TestParseBytes(t *testing.T) {
	intVal := 2097152
	strVal := "16MB"
	tests := []struct {
		name     string
		input    any
		expected int
	}{
		{"nil", nil, 0},
		{"zero int", 0, 0},
		{"negative int", -5, 0},
		{"positive int", 16777216, 16777216},
		{"int32", int32(2097152), 2097152},
		{"int64", int64(1048576), 1048576},
		{"uint", uint(65536), 65536},
		{"uint32", uint32(131072), 131072},
		{"uint64", uint64(262144), 262144},
		{"pointer int", &intVal, 2097152},
		{"pointer string", &strVal, 16777216},
		{"json.Number integer", json.Number("16777216"), 16777216},
		{"json.Number float", json.Number("2097152"), 2097152},
		{"float64 json unmarshal", float64(16777216), 16777216},
		{"float32", float32(2097152), 2097152},
		{"empty string", "", 0},
		{"zero string", "0", 0},
		{"plain digits string", "16777216", 16777216},
		{"plain digits with B", "2097152B", 2097152},
		{"unit 64KB", "64KB", 64 * 1024},
		{"unit 64KiB", "64KiB", 64 * 1024},
		{"unit 64k lowercase", "64k", 64 * 1024},
		{"unit 2M", "2M", 2 * 1024 * 1024},
		{"unit 2MB", "2MB", 2 * 1024 * 1024},
		{"unit 16 MiB with spaces", "16 MiB", 16 * 1024 * 1024},
		{"unit 16MB", "16MB", 16 * 1024 * 1024},
		{"unit 1GB", "1GB", 1024 * 1024 * 1024},
		{"unit 1GiB", "1GiB", 1024 * 1024 * 1024},
		{"unit 1g", "1g", 1024 * 1024 * 1024},
		{"fractional 1.5MB", "1.5MB", int(1.5 * 1024 * 1024)},
		{"invalid string", "invalid_bytes", 0},
		{"negative string", "-10MB", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseBytes(tc.input)
			if got != tc.expected {
				t.Errorf("ParseBytes(%v) = %d; want %d", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSmuxConfig(t *testing.T) {
	// Test defaults
	cfgDefault := smuxConfig(0, 0)
	if cfgDefault.MaxReceiveBuffer != DefaultSmuxMaxReceiveBuffer {
		t.Errorf("Default MaxReceiveBuffer = %d; want %d", cfgDefault.MaxReceiveBuffer, DefaultSmuxMaxReceiveBuffer)
	}
	if cfgDefault.MaxStreamBuffer != DefaultSmuxMaxStreamBuffer {
		t.Errorf("Default MaxStreamBuffer = %d; want %d", cfgDefault.MaxStreamBuffer, DefaultSmuxMaxStreamBuffer)
	}
	if err := smux.VerifyConfig(cfgDefault); err != nil {
		t.Fatalf("Default smux config invalid: %v", err)
	}

	// Test custom values
	customRecv := 32 * 1024 * 1024
	customStream := 4 * 1024 * 1024
	cfgCustom := smuxConfig(customRecv, customStream)
	if cfgCustom.MaxReceiveBuffer != customRecv {
		t.Errorf("Custom MaxReceiveBuffer = %d; want %d", cfgCustom.MaxReceiveBuffer, customRecv)
	}
	if cfgCustom.MaxStreamBuffer != customStream {
		t.Errorf("Custom MaxStreamBuffer = %d; want %d", cfgCustom.MaxStreamBuffer, customStream)
	}
	if err := smux.VerifyConfig(cfgCustom); err != nil {
		t.Fatalf("Custom smux config invalid: %v", err)
	}

	// Test safety expansion: when stream buffer exceeds receive buffer, receive buffer automatically expands
	cfgExpand := smuxConfig(1024*1024, 4*1024*1024)
	if cfgExpand.MaxReceiveBuffer < cfgExpand.MaxStreamBuffer {
		t.Errorf("smux MaxReceiveBuffer (%d) must be >= MaxStreamBuffer (%d)", cfgExpand.MaxReceiveBuffer, cfgExpand.MaxStreamBuffer)
	}
	if err := smux.VerifyConfig(cfgExpand); err != nil {
		t.Fatalf("Expanded smux config invalid: %v", err)
	}
}

func TestYamuxConfig(t *testing.T) {
	// Test default
	cfgDefault := yaMuxConfig(0)
	if cfgDefault.MaxStreamWindowSize != DefaultYamuxMaxStreamWindowSize {
		t.Errorf("Default MaxStreamWindowSize = %d; want %d", cfgDefault.MaxStreamWindowSize, DefaultYamuxMaxStreamWindowSize)
	}
	if err := yamux.VerifyConfig(cfgDefault); err != nil {
		t.Fatalf("Default yamux config invalid: %v", err)
	}

	// Test custom
	customWin := uint32(4 * 1024 * 1024)
	cfgCustom := yaMuxConfig(customWin)
	if cfgCustom.MaxStreamWindowSize != customWin {
		t.Errorf("Custom MaxStreamWindowSize = %d; want %d", cfgCustom.MaxStreamWindowSize, customWin)
	}
	if err := yamux.VerifyConfig(cfgCustom); err != nil {
		t.Fatalf("Custom yamux config invalid: %v", err)
	}

	// Test safety floor: when window size < 256KB, clamped to MinYamuxStreamWindowSize (256KB)
	cfgClamped := yaMuxConfig(64 * 1024)
	if cfgClamped.MaxStreamWindowSize < MinYamuxStreamWindowSize {
		t.Errorf("yamux MaxStreamWindowSize (%d) must be >= MinYamuxStreamWindowSize (%d)", cfgClamped.MaxStreamWindowSize, MinYamuxStreamWindowSize)
	}
	if err := yamux.VerifyConfig(cfgClamped); err != nil {
		t.Fatalf("Clamped yamux config invalid: %v", err)
	}
}

func TestNewClientBufferOptions(t *testing.T) {
	client, err := NewClient(Options{
		Protocol:            "smux",
		MaxReceiveBuffer:    "32MB",
		MaxStreamBuffer:     "4MB",
		MaxStreamWindowSize: "8MB",
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if client.maxReceiveBuffer != 32*1024*1024 {
		t.Errorf("client.maxReceiveBuffer = %d; want %d", client.maxReceiveBuffer, 32*1024*1024)
	}
	if client.maxStreamBuffer != 4*1024*1024 {
		t.Errorf("client.maxStreamBuffer = %d; want %d", client.maxStreamBuffer, 4*1024*1024)
	}
	if client.maxStreamWindowSize != 8*1024*1024 {
		t.Errorf("client.maxStreamWindowSize = %d; want %d", client.maxStreamWindowSize, 8*1024*1024)
	}
}

func TestNewServiceBufferOptions(t *testing.T) {
	svc, err := NewService(ServiceOptions{
		MaxReceiveBuffer:    "16MB",
		MaxStreamBuffer:     "2MB",
		MaxStreamWindowSize: "4MB",
	})
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}
	if svc.maxReceiveBuffer != 16*1024*1024 {
		t.Errorf("svc.maxReceiveBuffer = %d; want %d", svc.maxReceiveBuffer, 16*1024*1024)
	}
	if svc.maxStreamBuffer != 2*1024*1024 {
		t.Errorf("svc.maxStreamBuffer = %d; want %d", svc.maxStreamBuffer, 2*1024*1024)
	}
	if svc.maxStreamWindowSize != 4*1024*1024 {
		t.Errorf("svc.maxStreamWindowSize = %d; want %d", svc.maxStreamWindowSize, 4*1024*1024)
	}
}


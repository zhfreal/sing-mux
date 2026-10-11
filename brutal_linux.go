package mux

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/control"
	E "github.com/metacubex/sing/common/exceptions"
	"golang.org/x/sys/unix"
)

const (
	BrutalAvailable    = true
	TCP_BRUTAL_PARAMS  = 23301
	TCP_BRUTAL_VERSION = 23302
	BrutalVersionV2    = 0x020000
)

var (
	nativeEndian = binary.NativeEndian
	brutalCCName = [7]byte{'b', 'r', 'u', 't', 'a', 'l', 0}
)

func setsockoptRaw(fd int, level int, opt int, ptr unsafe.Pointer, length uintptr) error {
	_, _, errno := syscall.Syscall6(
		syscall.SYS_SETSOCKOPT,
		uintptr(fd),
		uintptr(level),
		uintptr(opt),
		uintptr(ptr),
		length,
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func packBrutalParamsV2(rate uint64, cwndGain uint32, groupID uint64) [20]byte {
	var b [20]byte
	nativeEndian.PutUint64(b[0:8], rate)
	nativeEndian.PutUint32(b[8:12], cwndGain)
	nativeEndian.PutUint64(b[12:20], groupID)
	return b
}

func packBrutalParamsV1(rate uint64, cwndGain uint32) [12]byte {
	var b [12]byte
	nativeEndian.PutUint64(b[0:8], rate)
	nativeEndian.PutUint32(b[8:12], cwndGain)
	return b
}

var cachedBrutalVersion int32 = -1

func getKernelBrutalVersion(fd int) int {
	v := atomic.LoadInt32(&cachedBrutalVersion)
	if v >= 0 {
		return int(v)
	}
	ver, verErr := unix.GetsockoptInt(fd, unix.IPPROTO_TCP, TCP_BRUTAL_VERSION)
	if verErr == nil && ver >= 0 {
		atomic.StoreInt32(&cachedBrutalVersion, int32(ver))
		return ver
	}
	atomic.StoreInt32(&cachedBrutalVersion, 0)
	return 0
}

func SetBrutalOptions(conn net.Conn, sendBPS uint64, groupID uint64, cwndGain ...uint32) error {
	syscallConn, loaded := common.Cast[syscall.Conn](conn)
	if !loaded {
		// Nested multiplexing or virtual tunnel stream: underlying transport manages congestion.
		// Allow graceful negotiation without erroring out.
		return nil
	}
	if sendBPS < BrutalMinSpeedBPS {
		sendBPS = BrutalMinSpeedBPS
	} else if sendBPS > BrutalMaxSpeedBPS {
		sendBPS = BrutalMaxSpeedBPS
	}
	gain := uint32(BrutalDefaultGain)
	if len(cwndGain) > 0 && cwndGain[0] > 0 {
		gain = cwndGain[0]
		if gain < BrutalMinCwndGain {
			gain = BrutalMinCwndGain
		} else if gain > BrutalMaxCwndGain {
			gain = BrutalMaxCwndGain
		}
	}
	return control.Conn(syscallConn, func(fd uintptr) error {
		err := setsockoptRaw(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, unsafe.Pointer(&brutalCCName[0]), uintptr(len(brutalCCName)))
		if err != nil {
			if errors.Is(err, unix.EPERM) {
				currentCC, getErr := unix.GetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION)
				if getErr != nil || currentCC != "brutal" {
					return os.NewSyscallError("setsockopt IPPROTO_TCP TCP_CONGESTION brutal", err)
				}
			} else {
				return E.Extend(
					os.NewSyscallError("setsockopt IPPROTO_TCP TCP_CONGESTION brutal", err),
					"please make sure you have installed the tcp-brutal kernel module",
				)
			}
		}

		ver := getKernelBrutalVersion(int(fd))
		if ver >= BrutalVersionV2 {
			buf := packBrutalParamsV2(sendBPS, gain, groupID)
			err = setsockoptRaw(int(fd), unix.IPPROTO_TCP, TCP_BRUTAL_PARAMS, unsafe.Pointer(&buf[0]), uintptr(len(buf)))
			runtime.KeepAlive(&buf)
		} else {
			buf := packBrutalParamsV1(sendBPS, gain)
			err = setsockoptRaw(int(fd), unix.IPPROTO_TCP, TCP_BRUTAL_PARAMS, unsafe.Pointer(&buf[0]), uintptr(len(buf)))
			runtime.KeepAlive(&buf)
		}
		if err != nil {
			if errors.Is(err, unix.EPERM) {
				return nil // Silently tolerate route/destination rules (congctl / brutalctl lock)
			}
			return os.NewSyscallError("setsockopt IPPROTO_TCP TCP_BRUTAL_PARAMS", err)
		}
		return nil
	})
}

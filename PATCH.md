# Codebase Change Statement: sing-mux

**Current Codebase:** `zhfreal/sing-mux` (`main-patched`)  
**Original Upstream Baseline:** `MetaCubeX/sing-mux` (Release `v0.3.12`, Git Commit `714fc2e0d3753578ee1beeb8f39943dc6c5aa426`)  
**Repository Location:** `./sing-mux-mine`  
**Status:** Production-Ready, Race-Verified (`go test -race ./...` passing 100%)

---

## 1. Executive Summary

This document specifies the complete architectural, functional, and performance delta between the current codebase (`zhfreal/sing-mux @ main-patched`) and the original upstream baseline (`MetaCubeX/sing-mux v0.3.12`).

The modifications are structured into two distinct categories:
1. **Type 1: New Features & Protocol Extensions**: Anti-fingerprinting periodic connection retirement (XMUX style), TCP Brutal v2 protocol negotiation with tenant isolation, transparent stream failover buffering, and half-close stream propagation.
2. **Type 2: Bug Fixes, Concurrency & Memory Safety Remediations**: Defensive front headroom reallocation preventing buffer overflow panics, unread payload byte draining on short buffers preventing stream desynchronization, separate read/write mutexes eliminating data races, HTTP/2 request pipe leak fixes, and hot-path zero-allocation refactoring.

---

## 2. Type 1: New Features & Protocol Extensions

### 2.1 XMUX-Style Periodic Session Retirement & Graceful Draining (`session.go`, `client.go`, `error.go`)
* **Session Lifecycle Wrapper (`sessionWrapper`)**:
  - Implemented `sessionWrapper` wrapping `abstractSession` to enforce dynamic retirement limits:
    - `HMaxReusableSecs`: Hard expiration deadline (`unreusableAt = now + HMaxReusableSecs`).
    - `HMaxRequestTimes`: Decrementing stream request quota (`leftRequests`).
    - `CMaxReuseTimes`: Decrementing session reuse quota (`leftReuseTimes`).
* **Lock-Free Compare-and-Swap (CAS) Quota Consumption**:
  - Implemented lock-free atomic CAS loops for `leftRequests` and `leftReuseTimes` decrements, preventing concurrent underflow from `0` to `-1` (the unlimited sentinel).
* **Graceful Active Stream Draining**:
  - In `Client.offer()`, expired or retired sessions are excluded from taking new streams.
  - Existing in-flight streams on retired sessions continue serving traffic uninterrupted until closed (`NumStreams() == 0`).
  - Upon closing the final active stream (`activeStreams.Add(-1) == 0`), `maybeCloseSession()` evicts and closes the retired session asynchronously, preventing resource leaks.

### 2.2 TCP Brutal v2 Protocol Negotiation & Multi-Tenant Isolation (`brutal.go`, `brutal_linux.go`, `client.go`, `server.go`)
* **Protocol Version 2 Negotiation**:
  - Upgraded kernel TCP Brutal integration to support Brutal Version 2 (port `23302`, magic `0x42525502`, setsockopt `TCP_BRUTAL_PARAMS_V2`).
  - Maintained backward-compatible fallback to Version 1 (port `23301`, magic `0x42525501`) when the server does not negotiate v2.
* **Wire Protocol Framing & Invariant**:
  - Transmits an 8-byte request frame (`ReceiveBPS uint64`) to enforce a zero-unread-byte invariant during initial stream handshakes.
  - Preserves explicitly configured `CwndGain` and `GroupID` on client and server instances when applying `SetBrutalOptions`.
* **Multi-Tenant Group ID Isolation**:
  - Derives a 64-bit tenant group ID using FNV-1a hashing over `(userUUID, clientGroupID)` to ensure fair bandwidth pacing across isolated tenant groups sharing a single server.
* **Cross-Platform Byte Serialization**:
  - Moved pure byte-packing functions `packBrutalParamsV2` and `packBrutalParamsV1` to platform-independent `brutal.go`, enabling cross-platform compilation and testing on Linux, Windows, and macOS (`amd64` and `arm64`).

### 2.3 Transparent Stream Swapping & Pre-Response Buffering (`client.go`, `client_conn.go`, `client_conn_wait.go`)
* **Enforced Wrapper Control (Omission of `Upstream() any`)**:
  - Deliberately omitted the `Upstream() any` method from `clientConn` and `clientConnWithCloseWrite`.
  - In upstream, external relay loops recursively unwrap connections implementing `Upstream() any` down to the physical `smux`/`yamux` stream. By omitting this method, external relay loops are forced to route I/O calls through `clientConn.Read()` and `Write()`, maintaining wrapper control for stream failover.
* **Initial Handshake Buffering & Transparent Stream Swapping**:
  - Implemented `firstWriteBuffer` (4 KiB capacity) in `clientConn` to capture client handshake bytes (e.g., TLS ClientHello) prior to receiving the server's response.
  - If a read error occurs before the handshake response is read (e.g. server reset, expired ticket, or network dropout), `clientConn` automatically invokes `CloseSession()` on the failed session, requests a fresh stream from the pool, and transparently replays `firstWriteBuffer`.
* **Targeted Session Isolation (`Client.CloseSession`)**:
  - Added `Session() abstractSession` to `wrapStream` and `wrapStreamCloseWrite`.
  - Added `Client.CloseSession(session)` to evict and terminate only the specific multiplexer session that encountered a failure, leaving all other concurrent sessions in the client pool intact.
* **Reader & Writer Synchronization During Failover**:
  - Introduced `retrying bool` and condition variable `retryCond *sync.Cond` to serialize concurrent readers: readers wait until the stream swapper finishes replaying initial headers on the new stream before resuming reads.

### 2.4 Half-Close Stream Support (`CloseWrite`) (`client_conn.go`, `h2mux_conn.go`)
* **Clean Half-Close Delegation**:
  - Implemented `clientConnWithCloseWrite` delegating `CloseWrite()` via thread-safe `c.getConn()`.
  - Maps half-close to `END_STREAM` on HTTP/2 (`httpConn`) and `FIN` on Yamux (`yamuxWrapStream`), preserving downlink response readability (e.g., SSE streaming, chunked HTTP POST responses) without resetting connections.

---

## 3. Type 2: Bug Fixes, Concurrency & Memory Safety Remediations

### 3.1 Defensive Front Headroom Reallocation (Buffer Overflow Panic Prevention) (`server_conn.go`, `client_conn.go`)
* **Elimination of `panic: buffer overflow`**:
  - In upstream, `serverPacketConn.WritePacket`, `serverPacketAddrConn.WritePacket`, `clientPacketConn.WriteBuffer`, and `clientPacketAddrConn.WritePacket` invoked `buffer.ExtendHeader(needed)` unconditionally, panicking with `buffer overflow` if the supplied buffer had zero front headroom.
  - Added automatic headroom checks: if `buffer.Start() < needed`, a new buffer is allocated (`buf.NewSize(buffer.Len() + needed)`), the payload is copied, and the original buffer is released safely.

### 3.2 Frame Alignment Preservation on `io.ErrShortBuffer` (Length Corruption Fix) (`client_conn.go`, `server_conn.go`)
* **Draining Unread Payload Bytes**:
  - In `clientPacketConn.Read`, `clientPacketConn.ReadFrom`, `serverPacketConn.ReadFrom`, and `serverPacketAddrConn.ReadFrom`, if the caller's slice is shorter than the frame length (`length > len(p)`), unread payload bytes are drained via `io.CopyN(io.Discard, conn, int64(length))` before returning `io.ErrShortBuffer`.
  - Prevents leftover payload bytes from desynchronizing the length headers of subsequent frames on the stream.

### 3.3 High-Concurrency Mutex Separation & Race Elimination (`client_conn.go`, `client_conn_wait.go`, `padding.go`)
* **Separation of Read and Write Locks**:
  - Added `readAccess sync.Mutex` alongside `access sync.Mutex` on `clientPacketConn` and `clientPacketAddrConn`. This separates read-path handshake verification from write operations, eliminating lock contention and removing data races on `c.responseRead` and `c.requestWritten`.
  - Added `writeAccess sync.Mutex` to `paddingConn` to serialize the first `kFirstPaddings` (16) padded frame writes across `write()`, `WriteBuffer()`, and `WriteVectorised()`, preventing data races on `c.writePadding`.
  - Added `c.access.Lock()` synchronization on initial request writes in `protocolConn.Write` and on server handshake verification.

### 3.4 HTTP/2 Request Pipe Leak Elimination on Failed Handshake (`h2mux.go`)
* **Immediate Pipe Teardown on RoundTrip Error**:
  - In `h2MuxClientSession.Open()`, if `s.transport.RoundTrip(request)` fails or returns a non-200 HTTP status, `pipeInWriter.CloseWithError(err)` is executed immediately.
  - Prevents leaked request body pipes and lingering goroutines when the remote HTTP/2 server rejects the multiplexed stream.
* **Stream Timeout Goroutine Elimination**:
  - Replaced the per-stream timeout goroutine, channel allocation (`requestDone := make(chan struct{})`), and `time.NewTimer` with a lightweight `time.AfterFunc(tcpTimeout, cancel)` stopped on response completion.

### 3.5 Elimination of Reflection Allocations across Protocol Hot Paths (`protocol.go`, `protocol_conn.go`, `padding.go`)
* **Zero-Allocation Stack Helpers**:
  - Implemented `readUint16(io.Reader) (uint16, error)` and `writeUint16(io.Writer, uint16) error` using stack-allocated `[2]byte` buffers.
  - Replaced all reflection-based `encoding/binary.Read` and `binary.Write` calls across stream request/response and packet parsing.
* **Coalesced Request Header Reading**:
  - `ReadRequest()` reads both `version` and `protocol` in a single `io.ReadFull(reader, header[:])` call instead of two separate single-byte `binary.Read` operations.
* **Vectorized Padding BigEndian Encoding**:
  - Replaced reflection `binary.Write` in `WriteVectorised` with direct `binary.BigEndian.PutUint16` encoding into `header.Extend(4)`.

### 3.6 Nil Underlying Connection Guards & Protocol Accounting Bounds (`client_conn.go`, `protocol_conn.go`, `padding.go`)
* **Nil-Pointer Safety Guards**:
  - Guarded `LocalAddr()`, `SetDeadline()`, `SetReadDeadline()`, `SetWriteDeadline()`, and `SyscallConn()` in `clientConn` against nil underlying connection references, returning `net.ErrClosed` or `syscall.ENOTSUP` instead of crashing.
  - Guarded `clientConnWithCloseWrite.CloseWrite()` to return `net.ErrClosed` when the underlying connection is nil.
* **Write Length Accounting Correction**:
  - In `protocolConn.Write(p)`, fixed accounting bug returning `len(p) + requestLen - 1` (which exceeded `len(p)` when `requestLen >= 2`). Now returns exact `n = len(p)` on success.
* **Zero-Data Padding Frame Resilience**:
  - In `paddingConn.Read` and `ReadBuffer`, handled zero-data padding frames (`originalDataSize == 0`) by skipping the padding bytes and advancing to the next frame instead of returning an unexpected `(0, nil)`.
* **Connection Teardown Condition Broadcasts & Buffer Cleanup**:
  - In `clientConn.Close()`, added condition variable broadcasts (`c.retryCond.Broadcast()`, `c.requestWriteCond.Broadcast()`, `c.responseReadCond.Broadcast()`) under `c.stateMu.Lock()` to immediately wake and terminate any concurrent reader or writer goroutines blocked in wait loops upon connection closure, preventing goroutine leaks.
  - Cleared `c.firstWriteBuffer = nil` on connection close so aborted handshakes immediately release initial handshake buffers, preventing memory retention.
* **Disjoint Mutex Hierarchy**:
  - In `clientConn.Read`, decoupled `c.getConn()` from `c.stateMu.Lock()`, guaranteeing that `connMu` is never held while acquiring `stateMu`, enforcing a strictly ordered, disjoint lock hierarchy.
* **Safe Retired Session Eviction**:
  - In `client.go`, relaxed retirement drain checks to `wrapper.activeStreams.Load() <= 0 && wrapper.NumStreams() <= 0`, ensuring non-positive stream counts are immediately and cleanly evicted from active connection tracking.
* **Kernel Version Cache Poisoning Fix**:
  - Cached kernel version in `cachedBrutalVersion` only when `ver > 0`, avoiding cache poisoning from pre-connect `getsockopt` calls.
  - Tolerates `EPERM` without error logging when kernel policy route locks are active.

### 3.7 Server Packet Write Coalescing & Frame Fragmentation Fix (`server_conn.go`)
* **Single-Buffer Frame Serialization**:
  - Coalesced the initial `statusSuccess` (1 byte), length prefix (2 bytes), and UDP payload into a single allocated `buf.Buffer` write.
  - Eliminates the generation of 2 to 3 separate physical TCP frames per outbound UDP datagram.

---

## 4. File Modification & Addition Matrix

| File Path | Status | Change Type | Description of Delta Relative to Upstream Baseline |
| :--- | :--- | :--- | :--- |
| `session.go` | **Added** | **Type 1** (New Feature) | Implemented `sessionWrapper` tracking active streams, retirement deadlines, and atomic CAS quota consumption. |
| `brutal.go` | **Modified** | **Type 1** (New Feature) | Port `23302` signaling, magic `0x42525502`, FNV-1a tenant group ID derivation, cross-platform `packBrutalParamsV1` / `packBrutalParamsV2`. |
| `brutal_linux.go` | **Modified** | **Type 1** & **Type 2** | Kernel v2 parameter struct (`tcp_brutal_params_v2`), setsockopt wrapper, positive-only version caching (`ver > 0`), EPERM tolerance. |
| `brutal_stub.go` | **Modified** | **Type 2** (Portability) | Cross-platform stubs for non-Linux operating systems. |
| `client.go` | **Modified** | **Type 1** & **Type 2** | Type 1: `CloseSession()` targeted eviction, async `Reset()`, XMUX retirement limits (`HMaxReusableSecs`, `HMaxRequestTimes`, `CMaxReuseTimes`), CwndGain preservation, user-tunable `MaxDrainRetries`, `DrainRetryInterval`, and `MaxStreamSwapRetries` options.<br>Type 2: Retired session eviction clamp (`<= 0`), named constants for `DefaultTCPTimeout`, `DefaultMinStreams`, `MaxDrainRetries`, and `DrainRetryInterval`. |
| `client_conn.go` | **Modified** | **Type 1** & **Type 2** | Type 1: Omission of `Upstream() any`, `firstWriteBuffer` handshake replay.<br>Type 2: `readAccess` / `access` mutex split, `io.ErrShortBuffer` payload draining, zero-alloc integer helpers, `Close()` condition broadcasts, `firstWriteBuffer = nil` memory release, named constants `MaxHandshakeBufferCapacity` (4096), `MaxStreamSwapRetries` (2), and `StreamSwapDialTimeout` (1s), with dynamic retrieval via `c.getMaxStreamSwapRetries()`. |
| `client_conn_wait.go` | **Modified** | **Type 2** (Bugfix) | `readAccess` mutex synchronization on `WaitReadPacket()`, stack `readUint16` decoding. |
| `error.go` | **Added** | **Type 1** (New Feature) | Sentinel error declarations (`ErrSessionClosed`, `ErrMaxRequestsReached`, etc.). |
| `h2mux.go` | **Modified** | **Type 2** (Bugfix) | Immediate `pipeInWriter.CloseWithError` on failure, `time.AfterFunc` stream timeout optimization, `http.StatusOK` status check. |
| `h2mux_conn.go` | **Modified** | **Type 1** (New Feature) | Half-close write delegation mapping to HTTP/2 `END_STREAM`. |
| `padding.go` | **Modified** | **Type 2** (Bugfix) | `writeAccess sync.Mutex` serialization, zero-data padding frame skipping, front/rear headroom reallocation guards, named constants for `MaxPaddingPayloadLen`, `MinPaddingLength`, `MaxRandomPaddingSpan`, and `PaddingHeaderLength`. |
| `protocol.go` | **Modified** | **Type 2** (Bugfix/Perf) | `readUint16` / `writeUint16` stack helpers, coalesced 2-byte header reads, zero-allocation `StreamRequest` serialization. |
| `protocol_conn.go` | **Modified** | **Type 2** (Bugfix) | `c.access.Lock()` synchronization on initial request write, exact write length accounting (`n = len(p)`). |
| `server.go` | **Modified** | **Type 1** & **Type 2** | Brutal v2 server-side parameter extraction, configurable CwndGain application, maxStreams bounds enforcement. |
| `server_conn.go` | **Modified** | **Type 2** (Bugfix) | Defensive front headroom reallocation, status/length/payload write coalescing, `c.access.Lock()` synchronization on handshake methods. |
| `brutal_test.go` | **Added** | Testing | Unit tests for Brutal v2 roundtrip, fallback, tenant derivation, and byte packing. |
| `client_conn_test.go` | **Added** | Testing | Unit tests for stream failover, zero headroom guards, and `CloseWrite` preservation. |
| `client_test.go` | **Added** | Testing | Unit tests for session retirement rollover and targeted session isolation. |
| `padding_test.go` | **Added** | Testing | Unit tests for zero-data padding frames and buffer reallocation. |
| `server_conn_test.go` | **Added** | Testing | Unit tests for server packet connection zero-headroom handling. |
| `yamux_halfclose_test.go`| **Added** | Testing | Unit test verifying half-close write preservation on Yamux streams. |

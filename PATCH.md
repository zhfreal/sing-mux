# Codebase Change Statement: sing-mux

**Current Codebase:** `zhfreal/sing-mux` (`main-patched`, Tag: `v0.3.12-patch12` / `v0.3.12+patch12`)  
**Original Upstream Baseline:** `MetaCubeX/sing-mux` (Release `v0.3.12`, Git Commit `714fc2e099f227b7b4bc5a62880bcb5d3c8d5b2f`)  
**Repository Location:** `./sing-mux-mine`  
**Status:** Production-Ready, Race-Verified (`go test -race ./...` passing 100%)

---

## 1. Executive Summary

This document specifies the complete architectural, functional, and performance delta between the current codebase (`zhfreal/sing-mux @ main-patched`) and the original upstream baseline (`MetaCubeX/sing-mux v0.3.12`, commit `714fc2e099f227b7b4bc5a62880bcb5d3c8d5b2f`).

The modifications are structured into two distinct categories:
1. **Type 1: New Features & Protocol Extensions**:
   - XMUX-style periodic session retirement (`HMaxReusableSecs`, `HMaxRequestTimes`, `CMaxReuseTimes`) with lock-free atomic CAS quota consumption and graceful active-stream draining.
   - TCP Brutal v2 protocol negotiation (`23302` / `0x42525502`, `TCP_BRUTAL_PARAMS_V2`) with backward-compatible v1 fallback and FNV-1a multi-tenant `GroupID` isolation.
   - Transparent pre-response stream failover (`firstWriteBuffer` handshake replay and targeted `Client.CloseSession` eviction).
   - Configurable client and service operational tunables (`MaxDrainRetries`, `DrainRetryInterval`, `MaxStreamSwapRetries`, `StreamSwapDialTimeout`, `MaxHandshakeBufferCapacity`, `StreamOpenRetries`, `H2MuxIdleTimeout`).
2. **Type 2: Bug Fixes, Concurrency & Memory Safety Remediations**:
   - Defensive front/rear headroom reallocation preventing `panic: buffer overflow` on zero-headroom packet and padding buffers.
   - Critical slice bounds fix (`len(p) < int(length)` instead of `cap(p) < int(length)`) and unread payload byte draining on `io.ErrShortBuffer` across all client and server UDP packet readers.
   - Separate read and write mutexes (`readAccess`, `access`, `writeAccess`), serialized initial protocol handshake framing in `protocolConn` / `vectorisedProtocolConn`, and single-buffer packet write coalescing.
   - HTTP/2 request pipe leak elimination, closed-wrapper `buf.Buffer` release, ordered `h2MuxConnWrapper.Close()` teardown, and prompt release of `firstWriteBuffer`, `client`, and `ctx` references on `clientConn`.
   - Zero-allocation stack integer helpers (`readUint16` / `writeUint16`), single-pass linked-list session selection in `Client.offer()`, and lock-free atomic SplitMix64 PRNG (`fastRandIntn`) across padding and session retirement paths.

---

## 2. New Features

### 2.1 XMUX-Style Periodic Session Retirement & Graceful Draining (`session.go`, `client.go`, `error.go`)
* **Session Lifecycle Wrapper (`sessionWrapper`)**:
  - Wraps `abstractSession` to enforce randomized (`Range{Min, Max}`) or fixed session retirement thresholds:
    - `HMaxReusableSecs`: Hard reuse deadline (`unreusableAt = now + HMaxReusableSecs`), paired with a retirement timer (`time.AfterFunc`) invoking `Client.maybeCloseSession`.
    - `HMaxRequestTimes`: Decrementing stream request quota (`leftRequests`).
    - `CMaxReuseTimes`: Decrementing session reuse quota (`leftReuseTimes`).
* **Lock-Free Compare-and-Swap (CAS) Quota Consumption**:
  - `ConsumeRequest()` and `ConsumeReuse()` use atomic CAS loops on `leftRequests` and `leftReuseTimes`, preventing concurrent underflow from `0` to `-1` (the unlimited sentinel).
* **Graceful Active Stream Draining**:
  - `Client.offer()` excludes retired sessions from taking new streams and immediately closes/evicts retired sessions whose `activeStreams.Load() <= 0 && NumStreams() <= 0`.
  - Existing in-flight streams on retired sessions continue serving traffic uninterrupted until closed; each stream's `wrapStream.onClose` callback decrements `activeStreams` and triggers `maybeCloseSession()` to evict and close the session once active streams drain.

### 2.2 TCP Brutal v2 Protocol Negotiation & Multi-Tenant Isolation (`brutal.go`, `brutal_linux.go`, `brutal_stub.go`, `client.go`, `server.go`)
* **Protocol Version 2 Negotiation**:
  - Supports TCP Brutal Version 2 (port `BrutalExchangePortV2 = 23302`, magic `BrutalMagicV2 = 0x42525502`, kernel setsockopt `TCP_BRUTAL_PARAMS_V2` with 20-byte `tcp_brutal_params_v2` struct `{rate, cwnd_gain, group_id}`).
  - Maintains automatic fallback to Version 1 (port `23301`, magic `0x42525501`, 12-byte `tcp_brutal_params_v1`) when the peer or kernel only supports v1.
* **Wire Protocol Framing & Parameter Clamping**:
  - Transmits an 8-byte request frame (`BrutalRequestLen = 8`) and parses 9-byte v1 (`BrutalV1ResponseLen`) or 25-byte v2 (`BrutalV2ResponseLen`) success responses.
  - Clamps pacing rate to `[BrutalMinSpeedBPS (62500), BrutalMaxSpeedBPS (125000000000)]` and congestion window gain to `[BrutalMinCwndGain (5), BrutalMaxCwndGain (80)]` (default `BrutalDefaultGain = 20`).
* **Multi-Tenant Group ID Isolation**:
  - `deriveTenantGroupID(userUUID, clientGroupID)` hashes the authenticated user/source tenant identifier with the client-supplied group ID via 64-bit FNV-1a (`FNV64Offset`, `FNV64Prime`) when `AllowClientGroupID` is enabled on the server.
  - Extracts `*net.TCPAddr` IP bytes directly via `tAddr.IP.String()` when deriving fallback source tenant identity, avoiding `RemoteAddr().String()` + `net.SplitHostPort` allocations.
* **Multi-Wrapper Socket Unwrapping & Kernel Probing (`brutal_linux.go`)**:
  - `unwrapBrutalSyscallConn` traverses `RawConn() net.Conn`, `NetConn() net.Conn`, and `Upstream() any` wrappers before falling back to `common.Cast[syscall.Conn](conn)` so `setsockopt` reaches the underlying OS socket across listener and statistics wrappers.
  - Caches detected kernel Brutal version in `cachedBrutalVersion` only when `ver > 0`, and tolerates `EPERM` when kernel policy locks are active.

### 2.3 Transparent Stream Swapping & Pre-Response Buffering (`client.go`, `client_conn.go`, `wrap_stream.go`)
* **Enforced Wrapper Control (Omission of `Upstream() any`)**:
  - Deliberately omits `Upstream() any` from `clientConn` and `clientConnWithCloseWrite` so external relay loops route I/O through `clientConn.Read()` and `Write()` rather than unwrapping directly to the underlying `smux`/`yamux`/`h2mux` stream.
* **Initial Handshake Buffering & Transparent Stream Swapping**:
  - `clientConn` buffers initial outbound payload bytes up to `MaxHandshakeBufferCapacity` (default 4096 bytes) in `firstWriteBuffer` until the server's stream response header is received.
  - If a write or initial response read fails before `responseRead` becomes true, `trySwapConnection` evicts the failed session via `Client.CloseSession`, opens a fresh stream within `StreamSwapDialTimeout` (default 1s) up to `MaxStreamSwapRetries` (default 2), and replays `firstWriteBuffer`.
  - Synchronizes concurrent readers and writers across handshakes and stream swaps using `stateMu` and lazy-initialized condition variables (`requestWriteCond`, `responseReadCond`, `retryCond`).
* **Prompt Handshake Buffer & Reference Release**:
  - Immediately clears `firstWriteBuffer = nil`, `client = nil`, and `ctx = nil` once the initial stream response is read (`responseRead = true`), when write payload exceeds `MaxHandshakeBufferCapacity`, when stream swap retries are exhausted, or when `Close()` is called.
  - `Client.DialContext` does not store the caller's dial `context.Context` on `clientConn`, preventing long-lived TCP streams from pinning dial-scoped context values in memory.
* **Thread-Safe Half-Close Delegation (`clientConnWithCloseWrite`)**:
  - `clientConnWithCloseWrite.CloseWrite()` delegates via `N.CloseWrite(c.getConn())` so half-close operates safely even if `c.conn` was swapped during initial handshake retry.

---

## 3. Bug Fixes, Concurrency & Resource Safety

### 3.1 Critical Slice Bounds Fix (`len(p)` vs `cap(p)`) & Short-Buffer Draining (`client_conn.go`, `server_conn.go`)
* **Elimination of Backing-Array Overwrite on Sub-Sliced Buffers**:
  - Upstream checked `if cap(p) < int(length)` before `io.ReadFull(conn, p[:length])` in `clientPacketConn.Read`, `clientPacketConn.ReadFrom`, `clientPacketAddrConn.ReadFrom`, `serverPacketConn.ReadFrom`, and `serverPacketAddrConn.ReadFrom`. Passing a sub-sliced buffer with `len(p) < int(length) <= cap(p)` silently overwrote memory past `len(p)`.
  - Replaced all 5 checks with `if len(p) < int(length)` (and `if len(b) < int(length)`).
* **Frame Alignment Preservation on `io.ErrShortBuffer`**:
  - Drains the unread `length` bytes via `io.CopyN(io.Discard, conn, int64(length))` before returning `io.ErrShortBuffer`, preventing leftover datagram payload bytes from corrupting subsequent frame length headers.

### 3.2 Defensive Front/Rear Headroom Reallocation (`client_conn.go`, `server_conn.go`, `padding.go`)
* **Elimination of `panic: buffer overflow`**:
  - In `serverConn.WriteBuffer`, `serverPacketConn.WritePacket`, `serverPacketAddrConn.WritePacket`, `clientPacketConn.WriteBuffer`, and `clientPacketAddrConn.WritePacket`, checks `if buffer.Start() < needed` before calling `buffer.ExtendHeader(needed)`. When front headroom is insufficient, allocates a replacement `*buf.Buffer` of size `buffer.Len() + needed`, copies the payload, and releases the original buffer.
  - In `paddingConn.WriteBuffer`, checks `if buffer.Start() < PaddingHeaderLength || buffer.FreeLen() < paddingLen` and reallocates safely when either front headroom or tail capacity is insufficient.
  - Releases `buffer` on address serialization errors in `clientPacketAddrConn.WritePacket` and `serverPacketAddrConn.WritePacket`.

### 3.3 Mutex Separation, Wire Framing Serialization & Packet Coalescing (`client_conn.go`, `client_conn_wait.go`, `server_conn.go`, `protocol_conn.go`, `padding.go`)
* **Read/Write Lock Separation**:
  - Separate `readAccess sync.Mutex` and `access sync.Mutex` on `clientPacketConn`, `clientPacketAddrConn`, `serverPacketConn`, and `serverPacketAddrConn` serialize concurrent readers (including `WaitReadPacket`) independently of concurrent writers.
  - `writeAccess sync.Mutex` on `paddingConn` serializes the first `kFirstPaddings` (16) padded writes across `write()`, `WriteBuffer()`, and `WriteVectorised()`.
* **Protocol Handshake Framing Serialization & Exact Write Accounting (`protocol_conn.go`)**:
  - `protocolConn.Write` and `vectorisedProtocolConn.WriteVectorised` hold `c.access.Lock()` across encoding and writing the initial session `Request` header, setting `c.requestWritten = true` only after the write succeeds so concurrent writers cannot precede the session header on the wire.
  - Fixed `protocolConn.Write(p)` return count from `n - 1` (which returned `len(p) + requestHeaderLen - 1 > len(p)`) to exact `n = len(p)`.
* **Single-Buffer Packet Write Coalescing (`client_conn.go`, `server_conn.go`)**:
  - Coalesces status byte, destination address, 2-byte length prefix, and UDP payload into a single `buf.Buffer` write across `clientPacketConn.Write`, `clientPacketConn.WriteTo`, `clientPacketAddrConn.WriteTo`, `serverPacketConn.WriteTo`, and `serverPacketAddrConn.WriteTo`, avoiding 2–4 fragmented TCP writes per UDP packet and preventing partial-write framing desynchronization.

### 3.4 HTTP/2 Stream & Session Lifecycle Fixes (`h2mux.go`, `server.go`, `client.go`)
* **Immediate Pipe Teardown & Timer Optimization in `h2MuxClientSession.Open` (`h2mux.go`)**:
  - Replaces the per-stream timeout goroutine and channel with `time.AfterFunc(tcpTimeout, cancel)` stopped immediately upon `RoundTrip` return.
  - Closes `pipeInWriter.CloseWithError(...)` (and `response.Body.Close()` on non-200 status) immediately when `RoundTrip` fails or returns a non-`200 OK` status, preventing leaked `io.Pipe` instances.
* **Buffer Release & Ordered Teardown in `h2MuxConnWrapper` (`h2mux.go`)**:
  - `h2MuxConnWrapper.WriteBuffer` calls `buffer.Release()` before returning `net.ErrClosed` when `w.closed` is true.
  - `h2MuxConnWrapper.Close` guards against duplicate close calls and completes `w.ExtendedConn.Close()` before closing `w.done`, ensuring `h2MuxServerSession.ServeHTTP` does not return to `http2.Server` while `request.Body.Close()` is still running.
  - `h2MuxServerSession.ServeHTTP` selects on `<-request.Context().Done()` to close the stream wrapper when a client aborts the HTTP/2 stream.
* **Bounded Control-Plane Deadlines & Stream Teardown (`client.go`, `server.go`)**:
  - `Service.newConnection` closes `stream` immediately if `ReadStreamRequest` fails and `defer stream.Close()` for the handler lifetime.
  - `Client.brutalExchange` and `Service.newConnection` (for `BrutalExchangeDomain`) enforce both `stream.SetDeadline` and a fallback `time.AfterFunc` stream close timer (covering `h2mux` streams where `SetDeadline` is a no-op), preventing stalled Brutal handshakes from blocking `Client.offerNew` under `c.access.Lock()` or leaking server goroutines.
  - `Client.openStream` exits immediately when `ctx.Err() != nil` and evicts dead sessions via `c.CloseSession` on `session.Open` failure.
  - `Client.maybeCloseSession` exits immediately when `c.closed` is true so closed clients do not schedule background drain timers.

### 3.5 Zero-Data Padding Frame Handling, Zero-Alloc Helpers & Lock-Free PRNG (`padding.go`, `protocol.go`, `session.go`, `client.go`)
* **Zero-Data Padding Frame Skipping (`padding.go`)**:
  - `paddingConn.Read` and `paddingConn.ReadBuffer` loop past padded frames with `originalDataSize == 0` (draining their padding bytes on the next iteration) instead of returning a spurious `(0, nil)` EOF-like read.
* **Stack Integer Helpers & Coalesced Header Reads (`protocol.go`)**:
  - Replaces reflection-based `encoding/binary.Read` and `binary.Write` with stack-backed `readUint16` / `writeUint16` and direct `binary.BigEndian.PutUint16` operations, and reads the 2-byte `Request` header (`version`, `protocol`) in a single `io.ReadFull` call.
  - Fixes `streamRequestLen` to compute exact `StreamFlagsLen + AddrPortLen` without an unused extra byte.
* **Single-Pass Session Selection & Lock-Free SplitMix64 PRNG (`client.go`, `session.go`, `padding.go`, `protocol.go`)**:
  - `Client.offer()` traverses `c.connections` in a single linked-list pass without allocating intermediate session slices via `common.Filter`.
  - Replaces global mutex-contended `math/rand.Intn` calls with lock-free atomic SplitMix64 `fastRandIntn`.

---

## 4. File Modification & Addition Matrix

| File Path | Status | Change Type | Description of Delta Relative to Upstream Baseline |
| :--- | :--- | :--- | :--- |
| `brutal.go` | **Modified** | **Type 1** & **Type 2** | Brutal v2 port `23302` and magic `0x42525502`, `BrutalExchangeRequest` / `BrutalExchangeResponse` wire framing, FNV-1a `deriveTenantGroupID`, cross-platform `packBrutalParamsV1` / `packBrutalParamsV2`, rate/gain clamping, and named protocol constants. |
| `brutal_linux.go` | **Modified** | **Type 1** & **Type 2** | Kernel `tcp_brutal_params_v2` support, `unwrapBrutalSyscallConn` multi-wrapper traversal, positive-only kernel version caching (`ver > 0`), and `EPERM` tolerance. |
| `brutal_stub.go` | **Modified** | **Type 1** (Portability) | Updated `SetBrutalOptions` signature (`bps, groupID, gain`) for non-Linux targets. |
| `client.go` | **Modified** | **Type 1** & **Type 2** | XMUX retirement options, Brutal v2 client negotiation with timer-backed deadline enforcement, `CloseSession` targeted eviction, single-pass `offer()` session selection, context-cancellation short-circuiting in `openStream`, and closed-client guards. |
| `client_conn.go` | **Modified** | **Type 1** & **Type 2** | Omission of `Upstream() any`, `firstWriteBuffer` handshake replay and prompt buffer/reference release, `readAccess` / `access` mutex separation, `len(p) < int(length)` bounds fix and short-buffer draining, zero-headroom reallocation, and single-buffer packet write coalescing. |
| `client_conn_wait.go` | **Modified** | **Type 2** (Bugfix) | `readAccess` mutex synchronization and `readUint16` stack decoding in `WaitReadPacket`. |
| `error.go` | **Added** | **Type 1** (New Feature) | Sentinel error definitions for session retirement, stream open failures, and Brutal parameter validation. |
| `h2mux.go` | **Modified** | **Type 1** & **Type 2** | Configurable `idleTimeout`, immediate `pipeInWriter.CloseWithError` on `RoundTrip` failure/non-200 status, `time.AfterFunc` timeout timer, `buffer.Release()` on closed `WriteBuffer`, ordered `h2MuxConnWrapper.Close()` teardown, and `request.Context().Done()` cancellation handling. |
| `padding.go` | **Modified** | **Type 2** (Bugfix/Perf) | `writeAccess` mutex serialization, zero-data padding frame skipping, front/rear headroom reallocation in `WriteBuffer`, pre-sized vectorised buffer slice assembly, and lock-free `fastRandIntn` padding length selection. |
| `protocol.go` | **Modified** | **Type 2** (Bugfix/Perf) | `readUint16` / `writeUint16` stack helpers, coalesced 2-byte header read in `ReadRequest`, exact `streamRequestLen` calculation, and named header length constants. |
| `protocol_conn.go` | **Modified** | **Type 2** (Bugfix) | `c.access.Lock()` synchronization across initial handshake writes in `Write` and `WriteVectorised`, and exact `n = len(p)` write length accounting. |
| `server.go` | **Modified** | **Type 1** & **Type 2** | Brutal v2 server negotiation and tenant group ID derivation, configurable `H2MuxIdleTimeout`, timer-backed Brutal control stream deadline, nil-safe logger checks, and immediate/deferred stream closure. |
| `server_conn.go` | **Modified** | **Type 2** (Bugfix) | Defensive front headroom reallocation, `readAccess` / `access` mutex synchronization, `len(p) < int(length)` bounds fix and short-buffer draining, and single-buffer packet write coalescing. |
| `session.go` | **Modified** | **Type 1** & **Type 2** | `sessionWrapper` retirement state and lock-free CAS quota consumption, `Range` parser, lock-free SplitMix64 `fastRandIntn`, and `h2MuxIdleTimeout` plumbing. |
| `brutal_test.go` | **Added** | Testing | Unit tests for Brutal v1/v2 wire serialization, fallback, corrupt/truncated responses, tenant group ID hashing, and closed-client drain behavior. |
| `client_conn_test.go` | **Added** | Testing | Unit tests for `clientConnWithCloseWrite`, zero-headroom packet writes, closed-connection behavior, coalesced UDP writes, and handshake buffer/reference release. |
| `client_test.go` | **Added** | Testing | Unit tests for `parseRange`, `sessionWrapper` retirement rollover, `wrapStream` close callbacks, and `CloseSession` isolation. |
| `padding_test.go` | **Added** | Testing | Unit tests verifying `paddingConn.Read` and `ReadBuffer` skip zero-data padded frames cleanly. |
| `server_conn_test.go` | **Added** | Testing | Unit tests for server packet zero-headroom reallocation, nil-flusher HTTP/2 wrapper writes, and `len(p)` vs `cap(p)` short-buffer safety. |

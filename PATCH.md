# Patch Statement: sing-mux (v0.3.12-patch2)

This document describes the functional and architectural changes introduced in branch `main-patched` of `zhfreal/sing-mux` on top of the original upstream release `metacubex/sing-mux v0.3.12`.

---

## Module 1: Connection Resilience & Transparent Stream Re-routing

### 1. Issues in Original sing-mux v0.3.12
- **Relay Loop Stream Bypassing**: When Mihomo's `bufio.Copy` connection relay loops copy data, they inspect whether wrappers implement `Upstream() any` and recursively unwrap down to the underlying `smux` / `yamux` stream object. When an upstream proxy connection resets or a VLESS Reality session ticket expires, any reconnection swap on the wrapper is completely bypassed by the active copy loop, which continues to write to the dead underlying stream and terminates the user's connection.
- **Premature Concurrency on Reconnection Replay**: During an automatic stream retry, multiple readers or writers can trigger duplicate replaying of the handshake payload (`firstWriteBuffer`), causing protocol frame corruption or stream desynchronization.
- **Nil Connection Panics**: Wrapper methods on closed or uninitialized connections can panic with nil pointer dereferences if the underlying connection reference is nil.

### 2. Implementation Details (`client_conn.go`, `client.go`)
- **Omission of `Upstream() any`**: Deliberately omitted `Upstream() any` from `clientConn` and `clientConnWithCloseWrite`. This forces external relay loops to always route I/O calls through `clientConn.Read()` and `clientConn.Write()`, preserving wrapper control and enabling seamless underlying stream swapping.
- **Initial Handshake Buffering & Transparent Replay**:
  - Implemented `firstWriteBuffer` (buffering up to 4 KB) to capture pre-response client handshake payloads (e.g., TLS ClientHello).
  - On read failure before handshake response (e.g. expired Reality ticket or server reset), `clientConn` automatically invokes `Reset()` on the client, opens a fresh multiplexed stream, and transparently replays `firstWriteBuffer`.
- **Reader & Writer Synchronization**:
  - Introduced `retrying bool` flag and `retryCond *sync.Cond` to serialize readers during connection swaps: reader goroutines wait until the swapper has finished replaying headers and initial payloads before attempting reads on the new connection.
  - Synchronized state transitions via mutexes (`connMu`, `dialMu`, `stateMu`) and condition variables (`requestWriteCond`, `responseReadCond`), preventing concurrent duplicate write replay.
- **Nil Connection Guards**:
  - Protected `LocalAddr()`, `SetDeadline()`, `SetReadDeadline()`, `SetWriteDeadline()`, and `SyscallConn()` in `clientConn` against nil connection references, returning `net.ErrClosed` or `syscall.ENOTSUP` instead of crashing.
  - Guarded `clientConnWithCloseWrite.CloseWrite()` to return `net.ErrClosed` when the underlying connection is nil.
- **Timeout Tuning**:
  - Shortened default client `tcpTimeout` from 5 s to 500 ms to accelerate discovery of stale connections.
  - Reduced reconnection context timeout from 10 s to 1 s for prompt recovery.
  - Made `Client.Reset()` asynchronous (`go session.Close()`) to prevent session closure from blocking active dial retries.

---

## Module 2: XMUX-Style Periodic Session Retirement & Graceful Drain

### 1. Problem
Under persistent TCP traffic, long-lived multiplexed connections are vulnerable to stateful flow tracking, heuristic classification, and RST severance by censorship middleboxes.

### 2. Implementation Details (`client.go`, `session.go`, `error.go`)
- **Session Wrapper & Bounds**: Implemented `sessionWrapper` wrapping `abstractSession` to track:
  - `HMaxReusableSecs`: Hard expiration timestamp (`unreusableAt = now + HMaxReusableSecs`).
  - `HMaxRequestTimes`: Remaining stream request capacity (`leftRequests`).
  - `CMaxReuseTimes`: Remaining session reuse capacity (`leftReuseTimes`).
- **Graceful Drain Lifecycle**:
  - In `Client.offer()`, expired/retired sessions are excluded from taking new stream requests.
  - Active in-flight streams on retired sessions continue serving traffic uninterrupted until closed (`NumStreams() == 0`).
  - Retired sessions with 0 active streams are cleanly closed and evicted from the session list.
- **Stream Lifecycle Tracking across Multiplexers**:
  - Maintained `wrapStream.onClose` callback and `closeOnce` semantics across `smux`, `yamux`, and `h2mux`.
  - Closing a stream decrements `activeStreams.Add(-1)` and triggers `maybeCloseSession()` for timely retirement eviction without resource leaks.

---

## Module 3: Defensive Front Headroom Fallback & Buffer Safety

### 1. Problem
In `metacubex/sing-mux`, packet connection writing methods (`WritePacket` and `WriteBuffer`) prepend protocol framing headers via `buffer.ExtendHeader(...)`. If a caller supplies a `buf.Buffer` with insufficient front headroom (`buffer.Start() < needed`), `ExtendHeader` triggers an unhandled `panic: buffer overflow: capacity ..., start 0, need ...`, crashing the process.

### 2. Implementation Details (`server_conn.go`, `client_conn.go`)
- **Automatic Headroom Reallocation Fallback**:
  - Applied to `serverPacketConn.WritePacket`, `serverPacketAddrConn.WritePacket`, `clientPacketConn.WriteBuffer`, and `clientPacketAddrConn.WritePacket`.
  - Checks if `buffer.Start() < needed`. If headroom is insufficient, transparently reallocates a buffer with the required headroom (`buf.NewSize(buffer.Len() + needed)`), copies the payload, and safely releases the original buffer.
- **Short Buffer Stream Drain Fix**:
  - In `clientPacketConn.Read`, `clientPacketConn.ReadFrom`, and `clientPacketAddrConn.ReadFrom`, unread packet payload bytes are drained (`io.CopyN(io.Discard, c.conn, int64(length))`) before returning `io.ErrShortBuffer`.
  - Prevents leftover payload bytes from corrupting length headers of subsequent frames on the multiplexed stream.

---

## Module 4: Protocol Engine Safety & Resource Leak Hardening

### 1. Implementation Details (`h2mux.go`, `server.go`, `client.go`)
- **H2Mux Channel & Close Safety**:
  - Wrapped `close(s.done)` in `sync.Once` (`s.closeOnce.Do`) to prevent `"close of closed channel"` panics during concurrent session teardown.
  - Wrapped inbound stream dispatch in `ServeHTTP` with a `select` listening on `s.done` and `request.Context().Done()`, eliminating goroutine leaks on unbuffered channel sends when a session is terminating.
  - Converted bare `writer.(http.Flusher)` type assertion to a safe comma-ok check, and guarded `w.flusher.Flush()` against nil in `Write` and `WriteBuffer` to prevent nil pointer dereferences on non-flusher writers.
- **Timer Leak Elimination**: Replaced `time.After(tcpTimeout)` with `time.NewTimer` and explicit `timer.Stop()` upon request completion in `h2MuxClientSession.Open`, preventing timer accumulation on short-lived streams.
- **Nil Logger Guards**: Protected all `logger.Debug`, `logger.InfoContext`, and `logger.ErrorContext` calls (notably in `server.go` handle connection error path) against nil pointer dereferences.

---

## Module 5: Test Suite & Regression Verification

Comprehensive unit test coverage is implemented across:
- **`client_conn_test.go`**:
  - `TestClientConnWithCloseWrite`: Validates `N.WriteCloser` implementation, verifies exclusion of `Upstream() any`, and tests half-close vs full close lifecycle.
  - `TestWrapStreamCloseWrite`: Verifies half-close propagation across stream wrappers.
  - `TestClientPacketConnZeroHeadroom`: Tests automatic front headroom reallocation fallback on `clientPacketConn`.
  - `TestClientPacketAddrConnZeroHeadroom`: Tests automatic front headroom reallocation fallback on `clientPacketAddrConn`.
- **`client_test.go`**:
  - `TestParseRange`: Validates parsing of range strings (e.g. `"10-20"`, `"30"`, `"0"`).
  - `TestSessionWrapperRetirement`: Verifies retirement evaluation against requests, reuses, and time limits.
  - `TestWrapStreamCloseCallback`: Verifies stream close callback invocation.
  - `TestClient_SessionRetirementRollover`: Tests seamless rollover from retired sessions to new sessions.
- **`server_conn_test.go`**:
  - `TestServerPacketConnZeroHeadroom` & `TestServerPacketAddrConnZeroHeadroom`: Verifies server packet zero-headroom reallocation fallback.
  - `TestH2MuxConnWrapperNilFlusher`: Verifies `h2MuxConnWrapper` performs `Write` and `WriteBuffer` safely without panic when the flusher is nil.
- **`yamux_halfclose_test.go`**:
  - `TestYamuxWrapStreamCloseWriteKeepsReadOpen`: Verifies that yamux `CloseWrite` keeps the read side open and delivers peer responses intact.

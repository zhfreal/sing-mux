# Patch Statement: sing-mux client retry logic

This repository contains local patches on top of version `v0.3.10` of `metacubex/sing-mux` to fix stream reconnect failures during VLESS Reality ticket expiration or connection resets.

## Summary of Patches

1. **Re-routing Stream Boundary (`client_conn.go`)**:
   - Removed the `Upstream() any` method from `clientConn` wrapper.
   - **Reason**: When Mihomo's `bufio.Copy` connection relay loops start copying data, they check if a wrapper implements `Upstream() any` and recursively unwrap it to call I/O methods (`ReadFrom` / `WriteTo`) directly on the underlying `smux` / `yamux` stream object. When a ticket validation fails and the connection retries, `clientConn` replaces the internal stream reference. However, the active copy loops are still tied directly to the old stream, resulting in subsequent read/write failures on the closed stream. Removing `Upstream()` forces the relay loops to call `Read` and `Write` on `clientConn` itself, allowing transparent swapping of the underlying stream.

2. **Automatic Connection Retry & Session Reset (`client_conn.go`)**:
   - Added `firstWriteBuffer` buffering (up to 4KB) on `clientConn` to capture initial handshake payloads (e.g., TLS Client Hello).
   - In `Read()`, if a handshake failure (e.g. ticket expired) occurs, `Reset()` is called on the multiplexer client to close the old/stale session. A new multiplexed stream is opened, and the initial write buffer is replayed automatically.
   - Added retry mechanisms in `Write()` as well for failed stream writes.

3. **Asynchronous Session Closing (`client.go`)**:
   - Changed `Client.Reset()` to run `go session.Close()` asynchronously to ensure session closure does not block the active thread during Dial retry.
   - Ensured failed sessions are closed cleanly if `session.Open` fails in `openStream()`.

4. **Connection Setup & Retry Optimization (`client.go` & `client_conn.go`)**:
   - Shortened default client `tcpTimeout` from `5 * time.Second` to `500 * time.Millisecond` to reduce the time spent waiting on dead session streams.
   - Reduced retry context timeout in `client_conn.go` from `10 * time.Second` to `1 * time.Second` to allow faster reconnection recovery on connection resets.

5. **Thread-Safe Retry & Concurrency Protection (`client_conn.go`)**:
   - Added connection locks (`connMu`), dial lock (`dialMu`), and state lock (`stateMu`) to synchronize concurrent reading and writing threads.
   - Introduced condition variables (`requestWriteCond` and `responseReadCond`) to serialize request writes and response reads without holding locks during blocking I/O operations (preventing deadlocks).
   - Guarded retry loop swapping using a `(swapped, ok)` state check to prevent concurrent duplicate replaying of the `firstWriteBuffer` payload.

6. **Reader Serialization, Channel Safety & Nil Guards (August 2026 Audit)**:
   - **Reader Serialization on Conn Swap (`client_conn.go`)**: Added `retrying bool` flag and `retryCond *sync.Cond` to serialize readers and writers during connection swap and replay. Reader goroutines in `Read()` wait on `retryCond` so they do not attempt to read from a newly swapped connection before the swapper has finished replaying the request header and payload. Broadcast and flag reset are guaranteed across all error and success paths.
   - **`firstWriteBuffer` Accumulation**: Preserved write accumulation for all pre-response writes to guarantee complete replay upon reconnection.
   - **Atomic `Close()`, Channel Send Protection & Safe Type Assertion in `h2mux` (`h2mux.go`)**: Wrapped `close(s.done)` in `s.closeOnce.Do` to prevent `"close of closed channel"` panics on concurrent session closure. Wrapped `s.inbound <- conn` in `ServeHTTP` with a `select` listening on `s.done` and `request.Context().Done()`, eliminating goroutine hangs on unbuffered channel sends. Changed bare `writer.(http.Flusher)` type assertion to comma-ok pattern to prevent panics if the `ResponseWriter` does not implement `http.Flusher`.
   - **Nil Logger Guards (`client.go` & `server.go`)**: Protected all `logger.Debug` / `logger.InfoContext` calls against nil pointer dereferences.

7. **Stream Synchronization & Timer Leak Fixes (August 2026 Audit - Part 2)**:
   - **`io.ErrShortBuffer` Stream Drain Fix (`client_conn.go`)**: In `clientPacketConn.Read`, `clientPacketConn.ReadFrom`, and `clientPacketAddrConn.ReadFrom`, unread packet payload bytes are drained (`io.CopyN(io.Discard, c.conn, int64(length))`) before returning `io.ErrShortBuffer`. This prevents trailing packet bytes from corrupting the length headers of subsequent frames on the stream transport.
   - **Timer Leak Elimination in `h2mux` (`h2mux.go`)**: Replaced `time.After(tcpTimeout)` with `time.NewTimer` and explicit `timer.Stop()` upon request completion in `h2MuxClientSession.Open`, preventing goroutine timer accumulation on short-lived multiplexed streams.

8. **Defensive Front Headroom Fallback in `WritePacket` (`server_conn.go`)**:
   - **`ExtendHeader` Buffer Overflow Prevention**: In `serverPacketConn.WritePacket` and `serverPacketAddrConn.WritePacket`, the methods prepend protocol framing (`ExtendHeader(2)` for payload length, `ExtendHeader(1)` for `statusSuccess`, and `ExtendHeader(addrPortLen)` for target address). If a caller supplies a `buf.Buffer` with insufficient front headroom (`buffer.Start() < needed`), `ExtendHeader` triggers an unhandled `panic: buffer overflow: capacity ..., start 0, need 2`.
   - **Automatic Reallocation Fallback**: Added a defensive check (`buffer.Start() < needed`) that automatically reallocates a buffer with the required headroom (`buf.NewSize(buffer.Len() + needed)`), resizes it to reserve the headroom, copies the payload, and releases the original buffer. This guarantees that `WritePacket` never crashes with a buffer overflow panic regardless of caller buffer preparation.


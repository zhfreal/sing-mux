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

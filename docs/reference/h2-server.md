---
id: h2-server
title: Protocol-Independent Loom Server
---

The v4 server (`LoomServer`) serves HTTP/1.1 and HTTP/2 concurrently through
thick protocol engines. One application definition (`Routes`) is shared by all
engines; each engine owns its wire semantics (frames, flow control,
connections) and hands fully-decoded `Request` values to the shared
dispatcher.

Supported protocols:
- **H1**: strict incremental HTTP/1.1 (cleartext)
- **H2C**: cleartext HTTP/2 prior-knowledge / preface detection
- **H2**: HTTP/2 over TLS (ALPN negotiation)

Not supported:
- **H3/QUIC**: transport seam exists but no engine is installed; production
  configuration fails validation before any socket is bound.
- **CONNECT, forward proxying, transparent H1<->H2 byte translation,
  WebSockets, HTTP/1.0, h2c Upgrade, H1 pipelining**: not implemented.

## Application definition

One `Routes` value serves all protocols. The handler logic is protocol-agnostic:

```scala mdoc:compile-only
import zio.blocks.context.Context
import zio.http._

val routes: Routes[Any] =
  Routes(Route(RoutePattern.GET, Handler.succeed(Response.ok)))

val context = Context.empty
  .add(LoomServer(Connector(bind = BindAddress.localhost(8080))))

val handle = Server.serve(routes, context)
```

## Connector protocol sets

A `Connector` describes one network binding. The `protocol` field selects the
wire shape; the `transport` and `negotiation` fields describe how the
listener selects among protocols on a single port.

### H2C cleartext

```scala mdoc:compile-only
import zio.http._

val h2c = Connector(
  bind = BindAddress.localhost(8080),
  protocol = Protocol.H2C(),
)
// Validates: Right(())
```

H2C clients send the connection preface (`PRI * HTTP/2.0...`) immediately,
with no HTTP/1.1 Upgrade dance.

### H2 over TLS

```scala mdoc:compile-only
import zio.blocks.config.Secret
import zio.http._

val tls = TlsConfig(
  certChain = TlsSource.PemString(Secret("...")),
  privateKey = TlsSource.PemString(Secret("...")),
  alpnProtocols = List("h2"),
  alpnPolicy = AlpnPolicy.StrictH2,
)

val h2 = Connector(
  bind = BindAddress.localhost(8443),
  protocol = Protocol.H2(tls),
)
// Validates: Right(())
```

### Shared H1+H2 via TLS ALPN

There is no `Protocol.H1` case class. H1 over TLS is served by configuring
`Protocol.H2` with an ALPN offer list that includes `http/1.1`, and setting
`NegotiationPolicy.TlsAlpn`. The TLS layer offers both `h2` and `http/1.1`
ALPN ids; the listener dispatches to the matching engine:

```scala mdoc:compile-only
import zio.blocks.config.Secret
import zio.http._

val sharedTls = TlsConfig(
  certChain = TlsSource.PemString(Secret("...")),
  privateKey = TlsSource.PemString(Secret("...")),
  alpnProtocols = List("h2", "http/1.1"),
  alpnPolicy = AlpnPolicy.NegotiateH2Preferred,
)

val shared = Connector(
  bind = BindAddress.localhost(8443),
  protocol = Protocol.H2(sharedTls),
  negotiation = NegotiationPolicy.TlsAlpn,
)
// Validates: Right(())
```

### Shared H1+H2C via cleartext preface detection

On a cleartext port, the listener sniffs the first 24 bytes. An exact
H2-preface match selects H2C; any other opening bytes select H1. No
`Upgrade: h2c` header is involved:

```scala mdoc:compile-only
import zio.http._

val cleartext = Connector(
  bind = BindAddress.localhost(8080),
  protocol = Protocol.H2C(),
  negotiation = NegotiationPolicy.CleartextPreface,
)
// Validates: Right(())
```

## Engine registration

Thick protocol engines are registered explicitly on the `LoomServer`. Each
engine owns its wire semantics and connections. The shared dispatcher serves
all engines:

```scala mdoc:compile-only
import zio.http._

val h1Engine = new ProtocolEngine {
  val id: EngineId                        = EngineId("h1")
  val transportKind: TransportKind        = TransportKind.Tcp
  val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
  def drain(): Unit = ()
  def close(): Unit = ()
}

val h2Engine = new ProtocolEngine {
  val id: EngineId                        = EngineId("h2")
  val transportKind: TransportKind        = TransportKind.Tcp
  val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.H2)
  def drain(): Unit = ()
  def close(): Unit = ()
}

val server = LoomServer(Connector(bind = BindAddress.localhost(0)))
  .withEngine(h1Engine)
  .withEngine(h2Engine)
```

`EngineRegistry.build` validates before any socket is bound:
- **Duplicate engine ids**: deterministic `DuplicateEngineId` failure.
- **Duplicate protocols**: deterministic `DuplicateProtocol` failure
  (each protocol has exactly one owner).
- **Incompatible transports**: TCP and UDP engines may coexist (independent
  port namespaces); Unix sockets are exclusive.

```scala mdoc:compile-only
import zio.http._

val h1 = new ProtocolEngine {
  val id: EngineId                        = EngineId("h1")
  val transportKind: TransportKind        = TransportKind.Tcp
  val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
  def drain(): Unit = ()
  def close(): Unit = ()
}

val h2 = new ProtocolEngine {
  val id: EngineId                        = EngineId("h2")
  val transportKind: TransportKind        = TransportKind.Tcp
  val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.H2)
  def drain(): Unit = ()
  def close(): Unit = ()
}

val result = EngineRegistry.build(List(h1, h2))
// result: Right(EngineRegistry) with protocols {Http1, H2}
```

## Multi-connector serving

Multiple connectors bind to different ports or addresses. All are validated
before any socket is opened:

```scala mdoc:compile-only
import zio.blocks.config.Secret
import zio.http._

val multiTls = TlsConfig(
  certChain = TlsSource.PemString(Secret("...")),
  privateKey = TlsSource.PemString(Secret("...")),
)

val server = LoomServer(
  Connector(bind = BindAddress.localhost(8080), protocol = Protocol.H2C()),
).addConnector(
  Connector(bind = BindAddress.localhost(8443), protocol = Protocol.H2(multiTls)),
)
```

`Connector.bindConflicts` checks for OS-level binding collisions:
- TCP+TCP: conflict on same host+port (non-ephemeral).
- TCP+UDP: never conflict (independent port namespaces).
- Ephemeral ports (`0`): never conflict (OS assigns distinct ports).

## Drain and shutdown

`ServerHandle` coordinates graceful shutdown across all engines:

```scala mdoc:compile-only
import zio.blocks.context.Context
import zio.http._

val routes2: Routes[Any] =
  Routes(Route(RoutePattern.GET, Handler.succeed(Response.ok)))

val ctx = Context.empty
  .add(LoomServer(Connector(bind = BindAddress.localhost(0))))

val handle = Server.serve(routes2, ctx)

// Graceful: stop accepting, finish in-flight, then close
handle.shutdown()
handle.awaitShutdown()

// Or combined:
handle.shutdownAndWait()
```

- `shutdown()` is CAS-idempotent (exactly-once). It stops all listeners,
  drains every engine concurrently under one deadline, and force-closes
  only connections that miss the deadline.
- `awaitShutdown()` blocks until all engines reach the terminal state.
- `shutdownAndWait()` combines both calls.
- `registerShutdownHook()` adds a JVM shutdown hook that calls
  `shutdownAndWait()`.

## Strict H1 posture

The H1 engine is a strict incremental parser. It rejects:

- **Ambiguous framing**: `Transfer-Encoding` + `Content-Length` is rejected
  (`AmbiguousFraming`).
- **Malformed requests**: invalid HTTP method, missing URI, bad header
  syntax → 400 + close.
- **Unknown methods**: 501 + close.
- **CONNECT requests**: 501 + close (no tunneling).
- **Body too large**: 413 + close.
- **Idle timeout**: connection closed after `idleTimeout` (default 60s).

Sequential keep-alive only (no pipelining). Each request is a virtual-thread
deadline guard. The H1 engine cannot poison subsequent requests: a malformed
request closes the connection cleanly.

## Unsupported H3/QUIC

H3/QUIC has no installed engine. Production configuration must not advertise
or run it:

```scala mdoc:compile-only
import zio.blocks.config.Secret
import zio.http._

val h3 = Connector(
  bind = BindAddress.localhost(0),
  protocol = Protocol.H3(
    TlsConfig(
      certChain = TlsSource.PemString(Secret("...")),
      privateKey = TlsSource.PemString(Secret("...")),
    ),
  ),
)

h3.validate match {
  case Left(ConnectorFailure.H3NotAdvertised) => // expected
  case other => // unexpected
}
```

`Protocol.H3` is never advertised (no `AppProtocol` member), never selected
(no ALPN id), and never run. The `ProtocolSet.fromLegacy(Protocol.H3(...))`
returns `Left(ConnectorFailure.H3NotAdvertised)`.

Transport seam: `TransportKind.Udp` is defined for the future QUIC injection
point but every UDP binding fails validation with `TcpUdpMismatch` because
no QUIC-family protocol is advertised.

## TLS: `TlsConfig` ALPN and version pinning

```scala mdoc:compile-only
import zio.blocks.config.Secret
import zio.http._

val tlsDetail = TlsConfig(
  certChain = TlsSource.PemString(Secret("...")),
  privateKey = TlsSource.PemString(Secret("...")),
  alpnProtocols = List("h2"),
  alpnPolicy = AlpnPolicy.StrictH2,
  tlsVersions = List("TLSv1.3", "TLSv1.2"),
)
```

- The ALPN offer list comes from `TlsConfig.alpnProtocols` (default
  `List("h2")`) on every path, including a caller-provided
  `TlsSource.SslContext`: a raw context without ALPN fails fast instead of
  silently bypassing negotiation.
- `AlpnPolicy.StrictH2` (the default) rejects non-`h2` clients at the TLS
  layer with an `SSLHandshakeException`. `NegotiateH2Preferred` accepts the
  negotiated protocol.
- `tlsVersions` pins the accepted TLS versions (default
  `List("TLSv1.3", "TLSv1.2")`); older clients are rejected during the
  handshake.
- `requireClientAuth` enables mutual TLS: the handshake aborts when the peer
  sends no (or an untrusted) certificate.

## `Http2Config`: wire SETTINGS and bounds

`Http2Config` is the single source of truth for the H2 server's SETTINGS
frame and its internal limits:

| Field                | Default | Wire effect and enforcement                                            |
| -------------------- | ------- | ---------------------------------------------------------------------- |
| `maxConcurrentStreams` | `100` | Sent as `SETTINGS_MAX_CONCURRENT_STREAMS`; over-limit streams are refused with `REFUSED_STREAM` |
| `initialWindowSize`  | `65535` | Sent as `SETTINGS_INITIAL_WINDOW_SIZE`; values outside `[0, 2147483647]` are rejected at config time |
| `maxFrameSize`       | `16384` | Sent as `SETTINGS_MAX_FRAME_SIZE`; values outside `[16384, 16777215]` throw `IllegalArgumentException` |
| `maxHeaderListSize`  | `8192`  | Sent as `SETTINGS_MAX_HEADER_LIST_SIZE`; oversized header blocks are rejected with `RST_STREAM(ENHANCE_YOUR_CALM)` or `GOAWAY(PROTOCOL_ERROR)` |

## Timeouts: idle `GOAWAY` and request `RST_STREAM`

`Connector.idleTimeout` (default 60 seconds) drives graceful connection
shutdown: an idle connection receives `GOAWAY(NO_ERROR)` carrying the real
`lastStreamId` (never `Int.MaxValue`), drains, then closes TCP. Request
timeouts surface as `RST_STREAM(CANCEL)`. Both run on Loom virtual threads,
sharing the connection's single write lock.

## Hardening: bounds, trust, access log, raw-body tap

Operator reference for the general hardening knobs. Every default below is
read off `Connector` (`zio-http-server/shared/src/main/scala/zio/http/Connector.scala`,
companion `Default*` vals) or `ClientConfig`
(`zio-http-client/shared/src/main/scala/zio/http/ClientConfig.scala`) — not
guessed. The server knobs live on the top-level `Connector` and apply across
H1/H2 transports. Wire-only settings stay on `Http2Config` (see above).

| Knob | Default | Enforcement point | Violation signal |
| ---- | ------- | ----------------- | ---------------- |
| `maxRequestBodySize` | `1 MiB` (`1024L * 1024L`, `DefaultMaxRequestBodySize`) | Per-stream: DATA payloads accounted incrementally; declared `content-length` over the cap is rejected before any body bytes are read | Over cap: `RST_STREAM(FLOW_CONTROL_ERROR)` (H2) or 413 + close (H1); per-stream/connection, surviving siblings |
| `requestTimeoutMs` | `30000` (30 s, `DefaultRequestTimeoutMs`) | Whole-request deadline from stream start until the response completes. Non-positive disables | `RST_STREAM(CANCEL)` (H2) or close (H1); per-stream |
| `headerTimeoutMs` | `5000` (5 s, `DefaultHeaderTimeoutMs`) | Time-to-complete from stream start covering trailing `CONTINUATION` frames. Non-positive disables | `RST_STREAM(CANCEL)` (H2) or close (H1); per-stream |
| `bodyTimeoutMs` | `10000` (10 s, `DefaultBodyTimeoutMs`) | Time-to-complete from stream start until the full body arrives. Non-positive disables | `RST_STREAM(CANCEL)` (H2) or close (H1); per-stream |
| `trustedProxy` | default-deny (`TrustedProxyConfig.default`) | Forwarding headers honored only for allowlisted-CIDR or mTLS-authenticated peers; otherwise stripped | Spoofed headers ignored; client IP falls back to socket peer address |

Example:

```scala mdoc:compile-only
import zio.http._

val connector = Connector(
  bind = BindAddress.localhost(8080),
  protocol = Protocol.H2C(),
  maxRequestBodySize = Connector.DefaultMaxRequestBodySize, // 1 MiB
  requestTimeoutMs = Connector.DefaultRequestTimeoutMs,     // 30 s
  headerTimeoutMs = Connector.DefaultHeaderTimeoutMs,       // 5 s
  bodyTimeoutMs = Connector.DefaultBodyTimeoutMs,           // 10 s
  trustedProxy = TrustedProxyConfig(trustedCidrs = Set("10.0.0.0/8")),
)
```

### Isolation contract

Every bound/timeout violation above is per-stream: the offending stream is
reset with the listed code and sibling streams on the same connection keep
working. The one exception is a peer that violates the HPACK contract itself
(e.g. an undecodable header block): that corrupts connection-level
compression state, so the server tears the connection down instead of serving
anything on it.

### Migration note (strict defaults)

Defaults are strict: 1 MiB body cap (`maxRequestBodySize`), 10 s total body
timeout (`bodyTimeoutMs`), 8192-byte header cap (`maxHeaderListSize`). Raise
them via the general `Connector` knobs when legitimate traffic exceeds them.

## Migration from H2-only Loom

Existing H2-only applications compile unchanged. The `Connector` API is
backward-compatible: `Protocol.H2C()` and `Protocol.H2(tls)` continue to
work as before.

To add H1 support, register an H1 engine and switch the negotiation policy:

```scala mdoc:compile-only
import zio.http._

// Before: H2-only
val h2Only = Connector(
  bind = BindAddress.localhost(8080),
  protocol = Protocol.H2C(),
)

// After: H1+H2C shared via preface detection
val h1h2c = Connector(
  bind = BindAddress.localhost(8080),
  protocol = Protocol.H2C(),
  negotiation = NegotiationPolicy.CleartextPreface,
)
```

`Connector.validate` reports failures as typed `ConnectorFailure` values
instead of throwing, so callers can distinguish causes without parsing
exception messages.

## Server-sent events

`ServerSentEvent(data, event, id, retry)` models one SSE message;
`SseCodec.encode` frames it. `Sse.body` builds an unknown-length
`text/event-stream` body, and `Sse.response` adds `Content-Type:
text/event-stream` with `Cache-Control: no-cache`:

```scala mdoc:compile-only
import zio.blocks.streams.Stream
import zio.http._
import zio.http.sse.Sse._
import zio.http.sse.ServerSentEvent

val events: Stream[Nothing, ServerSentEvent] =
  Stream.fromIterable(List(ServerSentEvent("hello", event = Some("greeting"))))

val response: Response = Response.sse(events)
val body: Body = Body.sse(events)
```

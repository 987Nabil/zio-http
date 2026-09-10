package zio.http

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental

import zio.blocks.context.Context

import zio.http.h1.H1Transport
import zio.http.h2.H2Transport

/**
 * Todo 11: thick TLS dispatch engine serving H1 and H2 on one port.
 *
 * Owns a single [[LoomListener]] on a TLS (`Protocol.H2`) connector. The
 * listener offers exactly `TlsConfig.alpnProtocols` on the wire and terminates
 * TLS; each accepted connection dispatches through [[TlsAlpnDispatch.select]]:
 * `h2` runs on the confirmed H2 serving path, `http/1.1` on the confirmed H1
 * serving path, and unknown/empty negotiation returns at once — the handler
 * return closes the socket before any byte reaches a parser.
 *
 * No wire logic is forked: both serving paths are the same `serveAccepted`
 * methods the standalone transports use, so framing, limits, deadlines, proxy
 * trust and telemetry stay single-sourced. The delegates are never `start`ed
 * (they own no listener); this engine's `drain`/`close`/`awaitQuiescent` fan
 * out to both trackers, and the aggregate handle (`BoundProtocolEngine`) stops
 * the one listener first, as with every other engine.
 *
 * Registration: claims `Http1` and `H2`, so a registry holding a standalone H1
 * or H2 engine alongside this one fails fast with
 * `EngineRegistrationError.DuplicateProtocol` (see `EngineRegistry.build`).
 * Strict-H2 and TLS version pins are retained: they are enforced by the same
 * `LoomListener.createTlsSocket` every TLS transport uses.
 */
@experimental
final class H1H2TlsEngine[Ctx](
  routes: Routes[Ctx],
  context: Context[Ctx],
  val connector: Connector,
  defectHandler: DefectHandler,
  val id: EngineId = EngineId("h1h2-tls"),
) extends ProtocolEngine
    with QuiescentEngine {

  def transportKind: TransportKind = TransportKind.Tcp

  def supportedProtocols: Set[ProtocolId] = Set(ProtocolId.Http1, ProtocolId.H2)

  /**
   * Block up to `timeout` for in-flight connections on both delegates to
   * settle. The H1 wait runs first against the full timeout; H2 gets the
   * remainder, so the combined wait never exceeds `timeout` by more than
   * scheduling granularity.
   */
  def awaitQuiescent(timeout: Duration): Boolean = {
    val delegates = running.get()
    if (delegates == null) true
    else {
      val deadline  = System.nanoTime() + timeout.toNanos
      val h1Settled = delegates._1.awaitQuiescent(timeout)
      val remaining = deadline - System.nanoTime()
      val h2Settled = delegates._2.awaitQuiescent(Duration.ofNanos(math.max(0L, remaining)))
      h1Settled && h2Settled
    }
  }

  /** Graceful drain: both delegates finish in-flight work (H2 emits GOAWAY). */
  def drain(): Unit = {
    val delegates = running.get()
    if (delegates != null) {
      delegates._1.drain()
      delegates._2.drainAll()
    }
  }

  /** Force-close every tracked connection on both delegates immediately. */
  def close(): Unit = {
    val delegates = running.get()
    if (delegates != null) {
      try delegates._1.close()
      finally delegates._2.closeAll()
    }
  }

  /**
   * Start serving `connector` on one TLS port with ALPN dispatch. Fails fast
   * with [[InvalidConnector]] before any socket binds unless the connector is a
   * TLS (`Protocol.H2`) binding on TCP whose `alpnProtocols` offer list is an
   * explicit non-empty set of known `h2`/`http/1.1` ids.
   */
  def start(): BoundConnectorHandle =
    connector.bind match {
      case BindAddress.Tcp(host, port) =>
        val tls      = H1H2TlsEngine.tlsOf(connector)
        TlsAlpnDispatch.enabledSet(tls) match {
          case Left(failure) => throw InvalidConnector(failure)
          case Right(_)      => ()
        }
        val h1       = new H1Transport(routes, context, connector, defectHandler)
        val h2       = new H2Transport(routes, context, connector, defectHandler)
        if (!running.compareAndSet(null, (h1, h2)))
          throw new IllegalStateException("H1H2TlsEngine already started")
        val listener = new LoomListener(host, port, Some(tls), conn => dispatch(conn, tls, h1, h2))
        val bound    = listener.start()
        BoundConnectorHandle(
          BoundConnector(BoundAddress.Tcp(bound.host, bound.port), connector.protocol),
          bound.close,
          bound.isRunning,
          bound.stopAccepting,
        )
      case BindAddress.Unix(path)      =>
        throw new UnsupportedOperationException("Unix domain sockets are not implemented yet: " + path)
    }

  private val running: AtomicReference[(H1Transport[Ctx], H2Transport[Ctx])] =
    new AtomicReference[(H1Transport[Ctx], H2Transport[Ctx])]()

  private def dispatch(
    conn: AcceptedConnection,
    tls: TlsConfig,
    h1: H1Transport[Ctx],
    h2: H2Transport[Ctx],
  ): Unit =
    TlsAlpnDispatch.select(tls, conn.negotiatedAlpn) match {
      case Right(AppProtocol.H2)    => h2.serveAccepted(conn)
      case Right(AppProtocol.Http1) => h1.serveAccepted(conn)
      case _                        => () // Explicit unknown/empty policy: close before any parser.
    }
}

@experimental
object H1H2TlsEngine {

  /**
   * The TLS identity a dispatch engine serves. Non-TLS connectors fail with a
   * typed [[InvalidConnector]] (H3 names its own refusal: no engine is
   * installed, so production configuration must neither advertise nor run it).
   */
  private[http] def tlsOf(connector: Connector): TlsConfig =
    connector.protocol match {
      case Protocol.H2(tls, _)  => tls
      case Protocol.H3(_, _, _) => throw InvalidConnector(ConnectorFailure.H3NotAdvertised)
      case Protocol.H2C(_)      =>
        throw InvalidConnector(
          ConnectorFailure.PolicyMismatch(
            "H1H2TlsEngine requires a TLS Protocol.H2 connector for ALPN dispatch, but the connector is cleartext H2C",
          ),
        )
    }
}

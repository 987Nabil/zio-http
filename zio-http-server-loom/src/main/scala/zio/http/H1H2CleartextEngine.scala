package zio.http

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental

import zio.blocks.context.Context

import zio.http.h1.H1Transport
import zio.http.h2.H2Transport

/**
 * Todo 12: thick cleartext dispatch engine serving H1 and H2C on one port.
 *
 * Owns a single [[LoomListener]] on a cleartext (`Protocol.H2C`) connector.
 * Each accepted connection passes through the bounded 24-byte
 * [[CleartextPrefaceDispatch.sniff]]: the complete exact preface replays
 * exactly once into the confirmed H2 serving path, diverged bytes replay
 * exactly once into the confirmed strict-H1 path, and malformed near-prefaces,
 * EOF, timeouts, and read failures close before any byte reaches a parser.
 *
 * Admission: at most `connector.maxHalfPrefaceConnections` connections sniff
 * concurrently (a [[Semaphore]] `tryAcquire`, never a wait — excess closes
 * before reading a byte, so half-open preface floods cannot exhaust virtual
 * threads). The permit covers the sniff only: it releases at the decision,
 * before the serving path blocks for the connection lifetime. The sniff
 * deadline is `connector.prefaceTimeoutMs` (one-shot guard, no polling).
 *
 * No wire logic is forked: both serving paths are the same `serveAccepted`
 * methods the standalone transports use, so framing, limits, deadlines, proxy
 * trust and telemetry stay single-sourced. The delegates are never `start`ed
 * (they own no listener); this engine's `drain`/`close`/`awaitQuiescent` fan
 * out to both trackers, and the aggregate handle (`BoundProtocolEngine`) stops
 * the one listener first, as with every other engine.
 *
 * Registration: claims `Http1` and `H2C`, so a registry holding a standalone H1
 * or H2C engine alongside this one fails fast with
 * `EngineRegistrationError.DuplicateProtocol` (see `EngineRegistry.build`).
 * There is no h2c Upgrade on this path: `Upgrade: h2c` request headers are
 * ordinary H1 headers and the H1 engine answers in H1.
 */
@experimental
final class H1H2CleartextEngine[Ctx](
  routes: Routes[Ctx],
  context: Context[Ctx],
  val connector: Connector,
  defectHandler: DefectHandler,
  val id: EngineId = EngineId("h1h2-cleartext"),
) extends ProtocolEngine
    with QuiescentEngine {

  def transportKind: TransportKind = TransportKind.Tcp

  def supportedProtocols: Set[ProtocolId] = Set(ProtocolId.Http1, ProtocolId.H2C)

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
   * Start serving `connector` on one cleartext port with preface dispatch.
   * Fails fast with [[InvalidConnector]] before any socket binds unless the
   * connector is a cleartext (`Protocol.H2C`) binding on TCP whose negotiation
   * policy is `CleartextPreface`.
   */
  def start(): BoundConnectorHandle =
    connector.bind match {
      case BindAddress.Tcp(host, port) =>
        H1H2CleartextEngine.requireCleartext(connector)
        val h1       = new H1Transport(routes, context, connector, defectHandler)
        val h2       = new H2Transport(routes, context, connector, defectHandler)
        if (!running.compareAndSet(null, (h1, h2)))
          throw new IllegalStateException("H1H2CleartextEngine already started")
        val permits  = new Semaphore(connector.maxHalfPrefaceConnections)
        val listener = new LoomListener(host, port, None, conn => dispatch(conn, h1, h2, permits))
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
    h1: H1Transport[Ctx],
    h2: H2Transport[Ctx],
    permits: Semaphore,
  ): Unit = {
    // Admission cap: excess half-preface connections close before reading.
    if (!permits.tryAcquire()) return
    val outcome =
      try CleartextPrefaceDispatch.sniff(conn.input, connector.prefaceTimeoutMs)
      finally permits.release()
    outcome match {
      case CleartextPrefaceDispatch.SniffOutcome.ToH2(replay) =>
        h2.serveAccepted(conn.copy(input = replay))
      case CleartextPrefaceDispatch.SniffOutcome.ToH1(replay) =>
        h1.serveAccepted(conn.copy(input = replay))
      case CleartextPrefaceDispatch.SniffOutcome.Reject       => ()
    }
  }
}

@experimental
object H1H2CleartextEngine {

  /**
   * The cleartext identity a dispatch engine serves. TLS (`Protocol.H2`) and H3
   * connectors fail with a typed [[InvalidConnector]] — TLS dispatch belongs to
   * `H1H2TlsEngine`, H3 has no engine — as does any policy but
   * `CleartextPreface` (single-protocol cleartext belongs to the standalone
   * H1/H2C engines).
   */
  private[http] def requireCleartext(connector: Connector): Unit =
    connector.protocol match {
      case Protocol.H2C(_)      =>
        if (connector.negotiation != NegotiationPolicy.CleartextPreface)
          throw InvalidConnector(
            ConnectorFailure.PolicyMismatch(
              "H1H2CleartextEngine requires NegotiationPolicy.CleartextPreface, but the connector negotiates " +
                connector.negotiation,
            ),
          )
        else ()
      case Protocol.H2(_, _)    =>
        throw InvalidConnector(
          ConnectorFailure.UnexpectedTls(
            "H1H2CleartextEngine requires a cleartext Protocol.H2C connector for preface dispatch, but the connector is TLS",
          ),
        )
      case Protocol.H3(_, _, _) => throw InvalidConnector(ConnectorFailure.H3NotAdvertised)
    }
}

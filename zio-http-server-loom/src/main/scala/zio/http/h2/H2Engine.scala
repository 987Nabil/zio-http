package zio.http.h2

import scala.annotation.experimental

import zio.blocks.context.Context

import zio.http.{
  BoundConnectorHandle,
  Connector,
  DefectHandler,
  EngineId,
  LoomServerTelemetry,
  Protocol,
  ProtocolEngine,
  ProtocolId,
  Routes,
  ServerTelemetry,
  TransportKind,
}

/**
 * Thick HTTP/2 protocol engine (Todo 6): owns the H2 wire stack
 * ([[H2Transport]] framing/routing, [[H2Connection]] stream lifecycle,
 * [[H2ConnectionControl]] GOAWAY/RST_STREAM, [[FlowController]] windows) and
 * exposes it behind the [[ProtocolEngine]] contract.
 *
 * Application behavior stays defined once, in `Server.serve(routes, context)`:
 * routing runs through the shared [[zio.http.EngineDispatcher]], so the H1
 * engine (Todo 7) and this transport can never diverge on route handling,
 * defect mapping, or 404/500 fallbacks.
 *
 * Lifecycle mapping:
 *
 *   - [[drain]]: every owned connection emits GOAWAY(NO_ERROR) with its real
 *     highest processed stream id (RFC 9113 section 6.8), refuses subsequently
 *     opened streams with REFUSED_STREAM, and wakes every thread blocked in a
 *     flow-control or frame wait so in-flight work observes the drain promptly.
 *     In-flight streams run to completion.
 *   - [[close]]: force-close — parked flow and frame waiters fail fast instead
 *     of parking to their deadlines.
 */
@experimental
final class H2Engine[Ctx](
  routes: Routes[Ctx],
  context: Context[Ctx],
  val connector: Connector,
  defectHandler: DefectHandler,
  val id: EngineId = EngineId("h2"),
  sendWindowTimeoutMs: Long = FlowController.DefaultSendWindowTimeoutMs,
  telemetry: ServerTelemetry = LoomServerTelemetry.global,
) extends ProtocolEngine {

  private val transport = new H2Transport(routes, context, connector, defectHandler, sendWindowTimeoutMs, telemetry)

  def transportKind: TransportKind = TransportKind.Tcp

  def supportedProtocols: Set[ProtocolId] = H2Engine.protocolsFor(connector)

  /**
   * Start serving `connector` through the owned transport. The returned handle
   * closes the listener binding; [[drain]]/[[close]] act on the owned
   * connections (see [[zio.http.LoomServer.serve]], which wires both together
   * on shutdown).
   */
  def start(): BoundConnectorHandle = transport.start()

  def drain(): Unit = transport.drainAll()

  def close(): Unit = transport.closeAll()
}

@experimental
object H2Engine {

  /**
   * The [[ProtocolId]] set an H2 engine serves on `connector`, chosen by the
   * configured application protocol: cleartext serves H2C, TLS serves H2.
   * H3/QUIC has no engine yet (see the H3 transport seam): asking fails fast
   * instead of binding a socket the engine cannot serve.
   */
  def protocolsFor(connector: Connector): Set[ProtocolId] =
    connector.protocol match {
      case Protocol.H2C(_)      => Set(ProtocolId.H2C)
      case Protocol.H2(_, _)    => Set(ProtocolId.H2)
      case Protocol.H3(_, _, _) => throw new UnsupportedOperationException("H3/QUIC is not implemented yet")
    }
}

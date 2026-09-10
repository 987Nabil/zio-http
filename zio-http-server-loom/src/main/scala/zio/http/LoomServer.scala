package zio.http

import scala.annotation.experimental

import zio.blocks.context.Context
import zio.http.h2.H2Engine

@experimental
class LoomServer(
  connector: Connector,
  additionalConnectors: List[Connector] = Nil,
  defectHandler: DefectHandler = DefectHandler.default,
  engines: List[ProtocolEngine] = Nil,
) extends Server {

  def addConnector(c: Connector): LoomServer =
    new LoomServer(connector, c :: additionalConnectors, defectHandler, engines)

  def withDefectHandler(h: DefectHandler): LoomServer =
    new LoomServer(connector, additionalConnectors, h, engines)

  /**
   * Explicitly register a thick protocol engine served alongside the
   * connectors. Engines are validated in [[serve]] before any socket is bound:
   * duplicate ids, duplicate protocols and incompatible transports fail with a
   * deterministic [[EngineRegistrationError]].
   */
  def withEngine(engine: ProtocolEngine): LoomServer =
    new LoomServer(connector, additionalConnectors, defectHandler, engine :: engines)

  def withEngines(newEngines: List[ProtocolEngine]): LoomServer =
    new LoomServer(connector, additionalConnectors, defectHandler, newEngines ++ engines)

  override def serve[Ctx](routes: Routes[Ctx], context: Context[Ctx]): ServerHandle = {
    if (engines.nonEmpty)
      EngineRegistry.build(engines) match {
        case Left(error) => throw error
        case Right(_)    => ()
      }
    val allConnectors = connector :: additionalConnectors
    // H2 is served behind its ProtocolEngine in every case: a registered
    // H2Engine whose connector matches is started as-is (so its drain/close
    // act on the live serving connections); every other connector gets an
    // internally-created H2Engine with the serve-time routes and context.
    // Other engine implementations validate-then-serve exactly as in Todo 1
    // (their wire stack arrives in later todos).
    val bound         = allConnectors.map { c =>
      engines.collectFirst { case engine: H2Engine[_] if engine.connector == c => engine } match {
        case Some(registered) =>
          val started = registered.start()
          BoundConnectorHandle(
            started.binding,
            () => {
              started.close0()
              registered.close()
            },
            started.isRunning0,
          )
        case None             =>
          new H2Engine(routes, context, c, defectHandler).start()
      }
    }
    ServerHandle.live(bound)
  }
}

@experimental
object LoomServer {
  def apply(connector: Connector = Connector.default): LoomServer =
    new LoomServer(connector)
}

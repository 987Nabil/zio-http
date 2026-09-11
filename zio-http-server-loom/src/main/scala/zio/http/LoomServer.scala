package zio.http

import scala.annotation.experimental

import zio.blocks.context.Context
import zio.http.h1.H1Transport
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
    // Fail-before-bind: every connector is model-validated (H3, UDP, policy)
    // before any socket is opened. InvalidConnector wraps the typed
    // ConnectorFailure so callers never see raw UnsupportedOperationException
    // from H2Engine/H2Transport construction.
    allConnectors.foreach { c =>
      c.validate match {
        case Left(failure) => throw InvalidConnector(failure)
        case Right(_)      => ()
      }
    }
    // Aggregate multi-engine startup (Todo 8): every connector binds through
    // its engine in order, and a bind failure rolls back the engines bound so
    // far in reverse order (see AggregateServerHandle.bindEngines) — nothing
    // is leaked on partial startup. The returned handle stops every listener,
    // drains every engine concurrently under one deadline, and blocks
    // awaitShutdown until the terminal state.
    AggregateServerHandle.bindEngines(allConnectors.map(c => () => bindConnector(c, routes, context)))
  }

  /**
   * Bind one connector through its engine: a registered [[H1H2TlsEngine]],
   * [[H1H2CleartextEngine]], [[H2Engine]] or [[H1Transport]] built with this
   * very connector instance is started as-is (so its drain/close act on the
   * live serving connections); every other connector gets an internally-created
   * [[H2Engine]] with the serve-time routes and context. Other engine
   * implementations validate-then-serve exactly as in Todo 1 (their wire stack
   * arrives in later todos).
   *
   * The match is by reference (`eq`), not by value: two connectors can be
   * structurally equal (for example two ephemeral `localhost(0)` bindings)
   * while naming distinct sockets, so only the instance the engine was
   * constructed with selects it.
   */
  private def bindConnector[Ctx](
    connector: Connector,
    routes: Routes[Ctx],
    context: Context[Ctx],
  ): BoundProtocolEngine =
    engines.collectFirst {
      case engine: H1H2TlsEngine[_] if engine.connector eq connector       =>
        new BoundProtocolEngine(engine, engine.start())
      case engine: H1H2CleartextEngine[_] if engine.connector eq connector =>
        new BoundProtocolEngine(engine, engine.start())
      case engine: H2Engine[_] if engine.connector eq connector            =>
        new BoundProtocolEngine(engine, engine.start())
      case engine: H1Transport[_] if engine.connector eq connector         =>
        new BoundProtocolEngine(engine, engine.start())
    }.getOrElse {
      val internal = new H2Engine(routes, context, connector, defectHandler)
      new BoundProtocolEngine(internal, internal.start())
    }
}

@experimental
object LoomServer {
  def apply(connector: Connector = Connector.default): LoomServer =
    new LoomServer(connector)
}

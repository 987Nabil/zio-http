package zio.http.compat

import scala.annotation.experimental

import zio.blocks.context.Context
import zio.http.{Connector, DefectHandler, LoomServer, ProtocolEngine, Routes}
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine

/**
 * Todo 22: Scala 3 constructors for the Loom protocol engines.
 *
 * Thin delegation over the shared engine implementations with idiomatic Scala 3
 * syntax (bare constructor application). The Scala 2.13 mirror is
 * [[Scala2EngineCompat]], which spells out `new` explicitly for the 2.13 rules.
 * Behavior parity is pinned by `Scala3ProtocolConsumerSpec` /
 * `Scala213ProtocolConsumerSpec`.
 */
@experimental
object Scala3EngineCompat {

  /**
   * Cleartext H1 engine for `connector` (sequential keep-alive, strict codec).
   */
  def h1Engine[Ctx](
    routes: Routes[Ctx],
    context: Context[Ctx],
    connector: Connector,
    defectHandler: DefectHandler,
  ): H1Transport[Ctx] =
    H1Transport(routes, context, connector, defectHandler)

  /** H2 engine for `connector` (H2C on cleartext, H2 on TLS). */
  def h2Engine[Ctx](
    routes: Routes[Ctx],
    context: Context[Ctx],
    connector: Connector,
    defectHandler: DefectHandler,
  ): H2Engine[Ctx] =
    H2Engine(routes, context, connector, defectHandler)

  /** Registers `engines` on `server`; validated fail-fast at serve time. */
  def withEngines(server: LoomServer, engines: List[ProtocolEngine]): LoomServer =
    server.withEngines(engines)
}

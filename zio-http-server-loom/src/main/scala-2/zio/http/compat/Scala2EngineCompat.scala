package zio.http.compat

import scala.annotation.experimental

import zio.blocks.context.Context
import zio.http.{Connector, DefectHandler, LoomServer, ProtocolEngine, Routes}
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine

/**
 * Todo 22: Scala 2.13 constructors for the Loom protocol engines.
 *
 * Thin delegation over the shared engine implementations with identical
 * semantics. The Scala 3 mirror is [[Scala3EngineCompat]]; behavior parity is
 * pinned by `Scala213ProtocolConsumerSpec` / `Scala3ProtocolConsumerSpec`.
 *
 * Why this object exists: regular-class constructors require an explicit `new`
 * on Scala 2.13 while Scala 3 accepts the bare `H1Transport(...)` /
 * `H2Engine(...)` shape, so 2.13 call sites need version-safe entry points. No
 * Scala 3 API was changed or reduced to provide them.
 */
@experimental
object Scala2EngineCompat {

  /**
   * Cleartext H1 engine for `connector` (sequential keep-alive, strict codec).
   */
  def h1Engine[Ctx](
    routes: Routes[Ctx],
    context: Context[Ctx],
    connector: Connector,
    defectHandler: DefectHandler,
  ): H1Transport[Ctx] =
    new H1Transport[Ctx](routes, context, connector, defectHandler)

  /** H2 engine for `connector` (H2C on cleartext, H2 on TLS). */
  def h2Engine[Ctx](
    routes: Routes[Ctx],
    context: Context[Ctx],
    connector: Connector,
    defectHandler: DefectHandler,
  ): H2Engine[Ctx] =
    new H2Engine[Ctx](routes, context, connector, defectHandler)

  /** Registers `engines` on `server`; validated fail-fast at serve time. */
  def withEngines(server: LoomServer, engines: List[ProtocolEngine]): LoomServer =
    server.withEngines(engines)
}

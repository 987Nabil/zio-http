package zio.http.compat

import scala.annotation.experimental

import zio.http._

/**
 * Todo 22: Scala 2.13 entry points for the protocol-independent server model.
 *
 * Thin delegation over the shared sources — every helper forwards to the
 * authoritative API with identical semantics. The Scala 3 mirror is
 * [[Scala3ProtocolCompat]]; behavior parity between the two is pinned by
 * `Scala213ProtocolConsumerSpec` / `Scala3ProtocolConsumerSpec`.
 *
 * Why this object exists: the Scala 2.13 toolchain infers narrower element
 * types for `Set(...)`/`List(...)` literals than Scala 3 does, so a 2.13
 * consumer writing `EngineRegistry.build(List(h1, h2))` or comparing against
 * `Set(ProtocolId.Http1)` hits unification failures that Scala 3 accepts. The
 * helpers below carry the declared result types, so 2.13 call sites need no
 * local ascriptions. No Scala 3 API was changed or reduced to provide them.
 */
@experimental
object Scala2ProtocolCompat {

  /** Protocols served by one H1 engine. */
  val h1Protocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)

  /** Protocols served by one cleartext H2 engine. */
  val h2cProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.H2C)

  /** Protocols served by one TLS H2 engine. */
  val h2Protocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.H2)

  /** Protocols served by one shared-cleartext H1+H2C engine. */
  val h1H2cProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1, ProtocolId.H2C)

  /** Protocols served by one shared-TLS H1+H2 engine. */
  val h1H2Protocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1, ProtocolId.H2)

  /**
   * Validated engine registry with 2.13-safe inference: the parameter is
   * already typed, so mixed engine lists unify without call-site ascriptions.
   */
  def registryOf(engines: List[ProtocolEngine]): Either[EngineRegistrationError, EngineRegistry] =
    EngineRegistry.build(engines)

  /**
   * Single-engine registry without a call-site `List[ProtocolEngine]`
   * ascription.
   */
  def singleEngineRegistry(engine: ProtocolEngine): Either[EngineRegistrationError, EngineRegistry] =
    EngineRegistry.build(List[ProtocolEngine](engine))

  /** Validated protocol set with 2.13-safe inference on the member sequence. */
  def protocolSetOf(members: Seq[AppProtocol]): Either[ConnectorFailure, ProtocolSet] =
    ProtocolSet.fromSeq(members)
}

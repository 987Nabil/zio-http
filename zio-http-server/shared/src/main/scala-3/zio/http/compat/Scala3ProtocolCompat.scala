package zio.http.compat

import scala.annotation.experimental

import zio.http._

/**
 * Todo 22: Scala 3 entry points for the protocol-independent server model.
 *
 * Thin delegation over the shared sources with idiomatic Scala 3 syntax
 * (inferred literal element types, saved by the declared result types). The
 * Scala 2.13 mirror is [[Scala2ProtocolCompat]], which spells out the same
 * types explicitly for the 2.13 inference rules. Behavior parity between the
 * two is pinned by `Scala3ProtocolConsumerSpec` /
 * `Scala213ProtocolConsumerSpec`.
 */
@experimental
object Scala3ProtocolCompat {

  /** Protocols served by one H1 engine. */
  val h1Protocols: Set[ProtocolId] = Set(ProtocolId.Http1)

  /** Protocols served by one cleartext H2 engine. */
  val h2cProtocols: Set[ProtocolId] = Set(ProtocolId.H2C)

  /** Protocols served by one TLS H2 engine. */
  val h2Protocols: Set[ProtocolId] = Set(ProtocolId.H2)

  /** Protocols served by one shared-cleartext H1+H2C engine. */
  val h1H2cProtocols: Set[ProtocolId] = Set(ProtocolId.Http1, ProtocolId.H2C)

  /** Protocols served by one shared-TLS H1+H2 engine. */
  val h1H2Protocols: Set[ProtocolId] = Set(ProtocolId.Http1, ProtocolId.H2)

  /**
   * Validated engine registry; checks run in fixed order, first violation wins.
   */
  def registryOf(engines: List[ProtocolEngine]): Either[EngineRegistrationError, EngineRegistry] =
    EngineRegistry.build(engines)

  /** Single-engine registry. */
  def singleEngineRegistry(engine: ProtocolEngine): Either[EngineRegistrationError, EngineRegistry] =
    EngineRegistry.build(List(engine))

  /** Validated protocol set. */
  def protocolSetOf(members: Seq[AppProtocol]): Either[ConnectorFailure, ProtocolSet] =
    ProtocolSet.fromSeq(members)
}

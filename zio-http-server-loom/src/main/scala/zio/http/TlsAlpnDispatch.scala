package zio.http

import scala.annotation.experimental

/**
 * Todo 11: pure TLS ALPN selection for shared H1/H2 endpoints.
 *
 * Decides only — never touches sockets. The enabled set derives exactly from
 * `TlsConfig.alpnProtocols` (`h2` -> H2, `http/1.1` -> Http1); the negotiated
 * id dispatches to its protocol through the shared [[Negotiation]] policy, so
 * unknown and absent offers fail with typed [[ConnectorFailure]] instead of
 * downgrading silently. The listener offers exactly these ids on the wire (see
 * `LoomListener.createTlsSocket`); rejected selections close before any byte
 * reaches a parser (see `H1H2TlsEngine` and the `H2Transport` gate).
 *
 * Cleartext-only `H2C` has no ALPN id and is never selected here — it is
 * selected by cleartext preface detection (see `Negotiation.selectForPreface`,
 * Todo 12), not by ALPN.
 */
@experimental
object TlsAlpnDispatch {

  /**
   * The protocol set enabled by `tls.alpnProtocols`, in offer order. Unknown
   * ids fail with [[ConnectorFailure.UnknownAlpnProtocol]] carrying the
   * offending id; an empty offer list fails with
   * [[ConnectorFailure.EmptyProtocolSet]]; duplicates fail with
   * [[ConnectorFailure.DuplicateProtocol]].
   */
  def enabledSet(tls: TlsConfig): Either[ConnectorFailure, ProtocolSet] = {
    var members                          = List.empty[AppProtocol]
    var failed: Option[ConnectorFailure] = None
    var index                            = 0
    val offered                          = tls.alpnProtocols
    while (index < offered.length && failed.isEmpty) {
      AppProtocol.fromAlpnId(offered(index)) match {
        case Some(protocol) => members = members :+ protocol
        case None           => failed = Some(ConnectorFailure.UnknownAlpnProtocol(offered(index)))
      }
      index += 1
    }
    failed match {
      case Some(failure) => Left(failure)
      case None          => ProtocolSet.fromSeq(members)
    }
  }

  /**
   * Dispatches one negotiated ALPN id to its protocol. An absent or empty
   * negotiation fails with [[ConnectorFailure.NoAlpnOffered]]; an id no enabled
   * member advertises fails with [[ConnectorFailure.UnknownAlpnProtocol]];
   * `StrictH2` additionally rejects anything but `h2` explicitly (defense in
   * depth behind the handshake reject in `LoomListener.createTlsSocket`).
   */
  def select(tls: TlsConfig, negotiatedAlpn: Option[String]): Either[ConnectorFailure, AppProtocol] =
    negotiatedAlpn.filter(_.nonEmpty) match {
      case None     => Left(ConnectorFailure.NoAlpnOffered)
      case Some(id) =>
        if (tls.alpnPolicy == AlpnPolicy.StrictH2 && id != "h2")
          Left(ConnectorFailure.UnknownAlpnProtocol(id))
        else
          enabledSet(tls) match {
            case Left(failure) => Left(failure)
            case Right(set)    => Negotiation.negotiateAlpn(set, List(id))
          }
    }
}

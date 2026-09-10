package zio.http

import scala.annotation.experimental

import zio._
import zio.blocks.config.Secret
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 11 RED: pure TLS ALPN selection contract.
 *
 * Pins the per-connection decision before any socket exists:
 *   - the enabled set derives exactly from `TlsConfig.alpnProtocols` (`h2` ->
 *     H2, `http/1.1` -> Http1; unknown ids and empty sets fail typed);
 *   - `select` maps the negotiated ALPN id to its protocol exactly;
 *   - empty/absent negotiation fails with `NoAlpnOffered`, unknown ids with
 *     `UnknownAlpnProtocol` — never a silent downgrade;
 *   - `StrictH2` additionally rejects anything but `h2` even when the offer
 *     list would allow more (defense in depth behind the handshake reject).
 */
@experimental
object TlsAlpnDispatchSpec extends ZIOSpecDefault {

  private def tls(
    alpn: List[String],
    policy: AlpnPolicy = AlpnPolicy.NegotiateH2Preferred,
  ): TlsConfig =
    TlsConfig(
      certChain = TlsSource.PemString(Secret(TlsAlpnFixtures.TestCert)),
      privateKey = TlsSource.PemString(Secret(TlsAlpnFixtures.TestKey)),
      alpnProtocols = alpn,
      alpnPolicy = policy,
    )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TlsAlpnDispatchSpec")(
      test("enabled set maps h2 and http/1.1 offer ids exactly") {
        ZIO.attempt {
          val result = TlsAlpnDispatch.enabledSet(tls(List("h2", "http/1.1")))
          assertTrue(result == Right(ProtocolSet.h1h2))
        }
      },
      test("enabled set keeps offer order canonical and rejects duplicates") {
        ZIO.attempt {
          val single = TlsAlpnDispatch.enabledSet(tls(List("h2")))
          val dup    = TlsAlpnDispatch.enabledSet(tls(List("h2", "h2")))
          assertTrue(
            single == Right(ProtocolSet.h2Only),
            dup == Left(ConnectorFailure.DuplicateProtocol(AppProtocol.H2)),
          )
        }
      },
      test("enabled set rejects unknown offer ids and empty offer lists") {
        ZIO.attempt {
          val unknown = TlsAlpnDispatch.enabledSet(tls(List("h2", "spdy/3")))
          val empty   = TlsAlpnDispatch.enabledSet(tls(Nil))
          assertTrue(
            unknown == Left(ConnectorFailure.UnknownAlpnProtocol("spdy/3")),
            empty == Left(ConnectorFailure.EmptyProtocolSet),
          )
        }
      },
      test("select dispatches h2 and http/1.1 to their protocols") {
        ZIO.attempt {
          val cfg = tls(List("h2", "http/1.1"))
          assertTrue(
            TlsAlpnDispatch.select(cfg, Some("h2")) == Right(AppProtocol.H2),
            TlsAlpnDispatch.select(cfg, Some("http/1.1")) == Right(AppProtocol.Http1),
          )
        }
      },
      test("select fails empty and absent negotiation before any dispatch") {
        ZIO.attempt {
          val cfg = tls(List("h2", "http/1.1"))
          assertTrue(
            TlsAlpnDispatch.select(cfg, None) == Left(ConnectorFailure.NoAlpnOffered),
            TlsAlpnDispatch.select(cfg, Some("")) == Left(ConnectorFailure.NoAlpnOffered),
          )
        }
      },
      test("select fails unknown ids even when the offer list is broad") {
        ZIO.attempt {
          val cfg = tls(List("h2", "http/1.1"))
          assertTrue(
            TlsAlpnDispatch.select(cfg, Some("h3")) == Left(ConnectorFailure.UnknownAlpnProtocol("h3")),
            TlsAlpnDispatch.select(cfg, Some("spdy/3")) == Left(ConnectorFailure.UnknownAlpnProtocol("spdy/3")),
          )
        }
      },
      test("select refuses unlisted ids that the peer negotiated out of band") {
        ZIO.attempt {
          val cfg = tls(List("h2"))
          assertTrue(
            TlsAlpnDispatch.select(cfg, Some("http/1.1")) == Left(ConnectorFailure.UnknownAlpnProtocol("http/1.1")),
          )
        }
      },
      test("StrictH2 selects h2 and rejects everything else explicitly") {
        ZIO.attempt {
          val cfg = tls(List("h2"), AlpnPolicy.StrictH2)
          assertTrue(
            TlsAlpnDispatch.select(cfg, Some("h2")) == Right(AppProtocol.H2),
            TlsAlpnDispatch.select(cfg, Some("http/1.1")) == Left(
              ConnectorFailure.UnknownAlpnProtocol("http/1.1"),
            ),
            TlsAlpnDispatch.select(cfg, None) == Left(ConnectorFailure.NoAlpnOffered),
            TlsAlpnDispatch.select(cfg, Some("h3")) == Left(ConnectorFailure.UnknownAlpnProtocol("h3")),
          )
        }
      },
    ) @@ sequential
}

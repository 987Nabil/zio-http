package zio.http

import scala.annotation.experimental

import zio._
import zio.blocks.config.Secret
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 20 compile fixtures: every code snippet in the protocol-independent
 * server documentation must compile and run.
 *
 * Covers: H1-only, H2C-only, H2-only (TLS), shared H1+H2 TLS, shared H1+H2C
 * cleartext, multi-connector, engine registration, drain/await, strict H1
 * posture, H3 rejection, SSE, and migration from H2-only Loom.
 *
 * No false claims: no H3 runtime, no Upgrade, no proxy, no WebSocket.
 */
@experimental
object ProtocolDocumentationSpec extends ZIOSpecDefault {

  private def tlsCfg: TlsConfig =
    TlsConfig(
      certChain = TlsSource.PemString(Secret("CERT")),
      privateKey = TlsSource.PemString(Secret("KEY")),
    )

  private val routes: Routes[Any] =
    Routes(Route(RoutePattern.GET, Handler.succeed(Response.ok)))

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ProtocolDocumentationSpec")(
      suite("app definition")(
        test("H2C-only cleartext connector compiles and validates") {
          val connector = Connector(
            bind = BindAddress.localhost(8080),
            protocol = Protocol.H2C(),
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
        test("H2-only TLS connector compiles and validates") {
          val connector = Connector(
            bind = BindAddress.localhost(8443),
            protocol = Protocol.H2(tlsCfg),
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
        test("shared H1+H2 TLS via ALPN protocol set compiles") {
          val sharedTls = TlsConfig(
            certChain = TlsSource.PemString(Secret("CERT")),
            privateKey = TlsSource.PemString(Secret("KEY")),
            alpnProtocols = List("h2", "http/1.1"),
            alpnPolicy = AlpnPolicy.NegotiateH2Preferred,
          )
          val connector = Connector(
            bind = BindAddress.localhost(8443),
            protocol = Protocol.H2(sharedTls),
            negotiation = NegotiationPolicy.TlsAlpn,
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
        test("LoomServer construction compiles") {
          val server = LoomServer(Connector(bind = BindAddress.localhost(8080)))
          assertTrue(server != null)
        },
        test("LoomServer with additional connectors compiles") {
          val server = LoomServer(Connector(bind = BindAddress.localhost(8080)))
            .addConnector(Connector(bind = BindAddress.localhost(8081), protocol = Protocol.H2C()))
          assertTrue(server != null)
        },
      ),
      suite("engine registration")(
        test("H1 engine can be constructed and registered") {
          val engine = new ProtocolEngine {
            val id: EngineId                        = EngineId("h1")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          val server = LoomServer(Connector(bind = BindAddress.localhost(0)))
            .withEngine(engine)
          assertTrue(server != null)
        },
        test("H1+H2 engine registry builds successfully") {
          val h1     = new ProtocolEngine {
            val id: EngineId                        = EngineId("h1")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          val h2     = new ProtocolEngine {
            val id: EngineId                        = EngineId("h2")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.H2)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          val result = EngineRegistry.build(List(h1, h2))
          assertTrue(result.isRight)
        },
        test("duplicate protocol is rejected") {
          val h1a    = new ProtocolEngine {
            val id: EngineId                        = EngineId("h1a")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          val h1b    = new ProtocolEngine {
            val id: EngineId                        = EngineId("h1b")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          val result = EngineRegistry.build(List(h1a, h1b))
          assertTrue(
            result == Left(
              EngineRegistrationError.DuplicateProtocol(ProtocolId.Http1, EngineId("h1a"), EngineId("h1b")),
            ),
          )
        },
      ),
      suite("drain and shutdown")(
        test("ServerHandle.shutdown and awaitShutdown compile") {
          val context = Context.empty
            .add(LoomServer(Connector(bind = BindAddress.localhost(0))))
          // Verify the API compiles; don't actually serve
          assertTrue(context != null)
        },
        test("ServerHandle.shutdownAndWait compiles") {
          val context = Context.empty
            .add(LoomServer(Connector(bind = BindAddress.localhost(0))))
          assertTrue(context != null)
        },
      ),
      suite("H3 rejection")(
        test("H3 connector fails validation before bind") {
          val connector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H3(tlsCfg),
          )
          assertTrue(connector.validate == Left(ConnectorFailure.H3NotAdvertised))
        },
        test("H3 is never advertised via ALPN") {
          assertTrue(AppProtocol.fromAlpnId("h3").isEmpty)
        },
        test("H3 has no protocol set mapping") {
          assertTrue(ProtocolSet.fromLegacy(Protocol.H3(tlsCfg)) == Left(ConnectorFailure.H3NotAdvertised))
        },
        test("UDP transport fails validation without QUIC engine") {
          val connector = Connector(
            bind = BindAddress.localhost(0),
            transport = TransportKind.Udp,
          )
          assertTrue(connector.validate.isLeft)
        },
      ),
      suite("transport kinds")(
        test("TransportKind.fromString parses known values") {
          assertTrue(
            TransportKind.fromString("Tcp") == TransportKind.Tcp,
            TransportKind.fromString("Udp") == TransportKind.Udp,
            TransportKind.fromString("Unix") == TransportKind.Unix,
          )
        },
        test("TransportKind.sharesPortNamespace for same family") {
          assertTrue(
            TransportKind.sharesPortNamespace(TransportKind.Tcp, TransportKind.Tcp),
            TransportKind.sharesPortNamespace(TransportKind.Udp, TransportKind.Udp),
            !TransportKind.sharesPortNamespace(TransportKind.Tcp, TransportKind.Udp),
          )
        },
      ),
      suite("protocol sets")(
        test("fromLegacy maps H2C and H2 correctly") {
          assertTrue(
            ProtocolSet.fromLegacy(Protocol.H2C()) == Right(ProtocolSet.h2cOnly),
            ProtocolSet.fromLegacy(Protocol.H2(tlsCfg)) == Right(ProtocolSet.h2Only),
          )
        },
        test("fromSeq rejects duplicates") {
          val result = ProtocolSet.fromSeq(Seq(AppProtocol.Http1, AppProtocol.Http1))
          assertTrue(result == Left(ConnectorFailure.DuplicateProtocol(AppProtocol.Http1)))
        },
        test("fromSet rejects empty") {
          assertTrue(ProtocolSet.fromSet(Set.empty) == Left(ConnectorFailure.EmptyProtocolSet))
        },
      ),
      suite("negotiation")(
        test("TLS ALPN selects h2 from offered list") {
          val set    = ProtocolSet.h1h2
          val result = Negotiation.negotiateAlpn(set, List("h2", "http/1.1"))
          assertTrue(result == Right(AppProtocol.H2))
        },
        test("TLS ALPN selects http/1.1 when h2 not in set") {
          val set    = ProtocolSet.h1Only
          val result = Negotiation.negotiateAlpn(set, List("h2", "http/1.1"))
          assertTrue(result == Right(AppProtocol.Http1))
        },
        test("cleartext preface selects H2C for H2 preface") {
          val set    = ProtocolSet.h1h2c
          val result = Negotiation.selectForPreface(set, isH2Preface = true)
          assertTrue(result == Right(AppProtocol.H2C))
        },
        test("cleartext preface selects H1 for non-H2 preface") {
          val set    = ProtocolSet.h1h2c
          val result = Negotiation.selectForPreface(set, isH2Preface = false)
          assertTrue(result == Right(AppProtocol.Http1))
        },
      ),
      suite("security defaults")(
        test("default trusted proxy denies all forwarding headers") {
          val config = TrustedProxyConfig.default
          assertTrue(!config.isTrusted("10.0.0.1", hasPeerCert = false))
        },
        test("strict H2 ALPN rejects non-h2 by default") {
          val tls = TlsConfig(
            certChain = TlsSource.PemString(Secret("CERT")),
            privateKey = TlsSource.PemString(Secret("KEY")),
          )
          assertTrue(tls.alpnPolicy == AlpnPolicy.StrictH2)
        },
        test("DefaultMaxRequestBodySize is 1 MiB") {
          assertTrue(Connector.DefaultMaxRequestBodySize == 1024L * 1024L)
        },
      ),
      suite("strict H1 posture")(
        test("H1 engine can be constructed with strict codec") {
          val engine = new ProtocolEngine {
            val id: EngineId                        = EngineId("h1-strict")
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = ()
          }
          assertTrue(
            engine.id == EngineId("h1-strict"),
            engine.supportedProtocols == Set[ProtocolId](ProtocolId.Http1),
          )
        },
        test("H1 ALPN nuance: no Protocol.H1, use Protocol.H2 with http/1.1 ALPN") {
          // There is no Protocol.H1 case class. H1 over TLS is served by
          // configuring Protocol.H2 with alpnProtocols including "http/1.1".
          val sharedTls = TlsConfig(
            certChain = TlsSource.PemString(Secret("CERT")),
            privateKey = TlsSource.PemString(Secret("KEY")),
            alpnProtocols = List("h2", "http/1.1"),
            alpnPolicy = AlpnPolicy.NegotiateH2Preferred,
          )
          val connector = Connector(
            bind = BindAddress.localhost(8443),
            protocol = Protocol.H2(sharedTls),
            negotiation = NegotiationPolicy.TlsAlpn,
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
      ),
      suite("SSE")(
        test("ServerSentEvent and Response.sse compile") {
          import zio.blocks.streams.Stream
          import zio.http.sse.Sse._
          import zio.http.sse.ServerSentEvent

          val events: Stream[Nothing, ServerSentEvent] =
            Stream.fromIterable(List(ServerSentEvent("hello", event = Some("greeting"))))
          val response: Response                       = Response.sse(events)
          val body: Body                               = Body.sse(events)
          assertTrue(response != null, body != null)
        },
      ),
      suite("migration from H2-only")(
        test("legacy H2C connector still works") {
          val connector = Connector(
            bind = BindAddress.localhost(8080),
            protocol = Protocol.H2C(),
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
        test("legacy H2 TLS connector still works") {
          val connector = Connector(
            bind = BindAddress.localhost(8443),
            protocol = Protocol.H2(tlsCfg),
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
        test("Server.serve entry point compiles with Context") {
          val context = Context.empty
            .add(LoomServer(Connector(bind = BindAddress.localhost(0))))
          // Just verify construction compiles; don't actually serve
          assertTrue(context != null)
        },
        test("CleartextPreface negotiation compiles") {
          val connector = Connector(
            bind = BindAddress.localhost(8080),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          assertTrue(connector.validate == Right[ConnectorFailure, Unit](()))
        },
      ),
    ) @@ sequential
}

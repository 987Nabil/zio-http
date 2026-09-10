package zio.http

import java.net.Socket
import java.nio.charset.StandardCharsets
import javax.net.ssl.SSLSocket

import scala.annotation.experimental

import zio._
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 11: one TLS port dispatches H1 and H2 through ALPN.
 *
 * Live wire proofs against a real [[H1H2TlsEngine]] on an ephemeral loopback
 * port:
 *   - an independent H2 TLS client (JDK HTTP_2) negotiates `h2` and gets 200;
 *   - an independent H1 TLS client (raw socket offering `http/1.1`) negotiates
 *     `http/1.1` exactly and gets 200 on the SAME port;
 *   - a strict-H2 dispatch port still rejects an H1-only client at the TLS
 *     layer and keeps serving H2;
 *   - unknown ALPN fails at the TLS layer (`no_application_protocol`) and
 *     absent ALPN closes right after the handshake — both before any byte
 *     reaches a parser (EOF promptly, never an `HTTP/` response);
 *   - pinned TLS versions flow through the shared listener;
 *   - after every rejection a fresh good connection still serves (no dirty
 *     state), and an aborted H1 request never poisons the next one.
 *
 * Every wait is a bounded socket timeout or a latch-free request/response; no
 * sleeps anywhere.
 */
@experimental
object TlsAlpnIntegrationSpec extends ZIOSpecDefault {

  private val BodyText = "tls-dispatch-ok"

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      Handler.succeed(Response.text(BodyText)),
    ),
  )

  private def withDispatch[R](
    tls: TlsConfig,
  )(use: Int => ZIO[R, Throwable, TestResult]): ZIO[R & Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val connector = Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(tls))
          val engine    = new H1H2TlsEngine(okRoutes, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          Server.serve(okRoutes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, value) => value
          case other                      => throw new AssertionError("Expected TCP binding: " + other)
        }
        use(port)
      }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TlsAlpnIntegrationSpec")(
      test("H2 and H1 TLS clients share one port and both get 200") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            val h2Client                         = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
            val (h2Version, h2Status, h2Body)    = TlsAlpnFixtures.jdkGet(h2Client, port, "/ok")
            val (h1Negotiated, h1Status, h1Body) = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
            assertTrue(
              h2Version == java.net.http.HttpClient.Version.HTTP_2,
              h2Status == 200,
              h2Body == BodyText,
              h1Negotiated == "http/1.1",
              h1Status == 200,
              h1Body == BodyText,
            )
          }
        }
      },
      test("raw H1 over TLS negotiates http/1.1 exactly and routes") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            val (negotiated, status, body) = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
            assertTrue(negotiated == "http/1.1", status == 200, body == BodyText)
          }
        }
      },
      test("offer order is deterministic: client offering both gets h2 first") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            val negotiated = TlsAlpnFixtures.handshakeOnly(port, Array("http/1.1", "h2"))
            assertTrue(negotiated == "h2")
          }
        }
      },
      test("strict-H2 dispatch rejects an H1-only client at the TLS layer and keeps serving H2") {
        withDispatch(TlsAlpnFixtures.tlsConfig(List("h2"), AlpnPolicy.StrictH2)) { port =>
          ZIO.attemptBlocking(TlsAlpnFixtures.handshakeOnly(port, Array("http/1.1"))).exit.flatMap { exit =>
            val rejectedAtTls = exit match {
              case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[javax.net.ssl.SSLException])
              case _                   => false
            }
            ZIO.attemptBlocking {
              val h2Client              = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
              val (_, h2Status, h2Body) = TlsAlpnFixtures.jdkGet(h2Client, port, "/ok")
              (rejectedAtTls, exit.isFailure, h2Status, h2Body)
            }.map { case (rejectedAtTls, failed, h2Status, h2Body) =>
              assertTrue(rejectedAtTls, failed, h2Status == 200, h2Body == BodyText)
            }
          }
        }
      },
      test("unknown ALPN fails at the TLS layer before any parser sees bytes") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            // A fully-unknown offer has no overlap with the server list, so
            // the JDK server stack aborts the handshake itself with a fatal
            // no_application_protocol alert: the connection never completes,
            // let alone reaches a parser.
            val rawSocket = new Socket("127.0.0.1", port)
            rawSocket.setSoTimeout(5000)
            val sslSocket = TlsAlpnFixtures
              .trustAllContext()
              .getSocketFactory
              .createSocket(rawSocket, "127.0.0.1", port, false)
              .asInstanceOf[SSLSocket]
            try {
              val params   = sslSocket.getSSLParameters
              params.setApplicationProtocols(Array("spdy/3"))
              sslSocket.setSSLParameters(params)
              sslSocket.setUseClientMode(true)
              try {
                sslSocket.startHandshake()
                "completed"
              } catch {
                case _: javax.net.ssl.SSLException => "rejected"
              } finally {
                try sslSocket.close()
                catch { case _: Throwable => () }
              }
              // Server-close evidence: the alert plus FIN arrive promptly and
              // carry no HTTP. A downgrade would deliver HTTP bytes; a missing
              // close would block until the socket timeout and fail here.
              val received = TlsAlpnFixtures.drainToEof(rawSocket)
              (received, "closed")
            } finally {
              try rawSocket.close()
              catch { case _: Throwable => () }
            }
          }.map { case (received, _) =>
            val text = new String(received, StandardCharsets.UTF_8)
            assertTrue(!text.startsWith("HTTP/"))
          }
        }
      },
      test("unknown ALPN handshake itself is rejected with an SSL error") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking(TlsAlpnFixtures.handshakeOnly(port, Array("spdy/3"))).exit.map { exit =>
            val rejectedAtTls = exit match {
              case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[javax.net.ssl.SSLException])
              case _                   => false
            }
            assertTrue(exit.isFailure, rejectedAtTls)
          }
        }
      },
      test("absent ALPN closes before any parser sees bytes") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            val rawSocket = new Socket("127.0.0.1", port)
            rawSocket.setSoTimeout(5000)
            val sslSocket = TlsAlpnFixtures
              .trustAllContext()
              .getSocketFactory
              .createSocket(rawSocket, "127.0.0.1", port, false)
              .asInstanceOf[SSLSocket]
            try {
              sslSocket.setUseClientMode(true)
              sslSocket.startHandshake()
              val negotiated = sslSocket.getApplicationProtocol
              try sslSocket.close()
              catch { case _: Throwable => () }
              val received   = TlsAlpnFixtures.drainToEof(rawSocket)
              (negotiated, received)
            } finally {
              try rawSocket.close()
              catch { case _: Throwable => () }
            }
          }.map { case (negotiated, received) =>
            val text = new String(received, StandardCharsets.UTF_8)
            assertTrue(negotiated == "", !text.startsWith("HTTP/"))
          }
        }
      },
      test("pinned TLS versions flow through the shared listener for both protocols") {
        withDispatch(TlsAlpnFixtures.tlsConfig(tlsVersions = List("TLSv1.3"))) { port =>
          ZIO.attemptBlocking {
            val rawSocket = new Socket("127.0.0.1", port)
            rawSocket.setSoTimeout(5000)
            val sslSocket = TlsAlpnFixtures
              .trustAllContext()
              .getSocketFactory
              .createSocket(rawSocket, "127.0.0.1", port, true)
              .asInstanceOf[SSLSocket]
            try {
              val params          = sslSocket.getSSLParameters
              params.setApplicationProtocols(Array("h2"))
              sslSocket.setSSLParameters(params)
              sslSocket.setUseClientMode(true)
              sslSocket.startHandshake()
              val sessionProtocol = sslSocket.getSession.getProtocol
              val negotiated      = sslSocket.getApplicationProtocol
              (sessionProtocol, negotiated)
            } finally {
              try sslSocket.close()
              catch { case _: Throwable => () }
            }
          }.flatMap { case (sessionProtocol, negotiated) =>
            ZIO.attemptBlocking {
              val (h1Negotiated, h1Status, h1Body) = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
              (sessionProtocol, negotiated, h1Negotiated, h1Status, h1Body)
            }.map { case (sessionProtocol, negotiated, h1Negotiated, h1Status, h1Body) =>
              assertTrue(
                sessionProtocol == "TLSv1.3",
                negotiated == "h2",
                h1Negotiated == "http/1.1",
                h1Status == 200,
                h1Body == BodyText,
              )
            }
          }
        }
      },
      test("rejections leave no dirty state: good H1 and H2 follow on the same port") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking(TlsAlpnFixtures.handshakeOnly(port, Array("spdy/3"))).exit.flatMap { exit =>
            val rejectedAtTls = exit match {
              case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[javax.net.ssl.SSLException])
              case _                   => false
            }
            ZIO.attemptBlocking {
              // Poison attempt two: absent ALPN, closed post-handshake.
              val rawSocket                      = new Socket("127.0.0.1", port)
              rawSocket.setSoTimeout(5000)
              val plain                          = TlsAlpnFixtures
                .trustAllContext()
                .getSocketFactory
                .createSocket(rawSocket, "127.0.0.1", port, false)
                .asInstanceOf[SSLSocket]
              try {
                plain.setUseClientMode(true)
                plain.startHandshake()
              } finally {
                try plain.close()
                catch { case _: Throwable => () }
                try rawSocket.close()
                catch { case _: Throwable => () }
              }
              // Same port still routes both protocols byte-exact.
              val h2Client                       = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
              val (_, h2Status, h2Body)          = TlsAlpnFixtures.jdkGet(h2Client, port, "/ok")
              val (negotiated, h1Status, h1Body) = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
              (rejectedAtTls, exit.isFailure, h2Status, h2Body, negotiated, h1Status, h1Body)
            }.map { case (rejectedAtTls, failed, h2Status, h2Body, negotiated, h1Status, h1Body) =>
              assertTrue(
                rejectedAtTls,
                failed,
                h2Status == 200,
                h2Body == BodyText,
                negotiated == "http/1.1",
                h1Status == 200,
                h1Body == BodyText,
              )
            }
          }
        }
      },
      test("aborted H1 request never poisons the next request") {
        withDispatch(TlsAlpnFixtures.tlsConfig()) { port =>
          ZIO.attemptBlocking {
            // Open a TLS H1 connection, send a partial request, abort mid-way.
            val rawSocket = new Socket("127.0.0.1", port)
            rawSocket.setSoTimeout(5000)
            val sslSocket = TlsAlpnFixtures
              .trustAllContext()
              .getSocketFactory
              .createSocket(rawSocket, "127.0.0.1", port, false)
              .asInstanceOf[SSLSocket]
            try {
              val params = sslSocket.getSSLParameters
              params.setApplicationProtocols(Array("http/1.1"))
              sslSocket.setSSLParameters(params)
              sslSocket.setUseClientMode(true)
              sslSocket.startHandshake()
              val out    = sslSocket.getOutputStream
              out.write("GET /ok HTTP/1.1\r\nHost: 127.0.0".getBytes(StandardCharsets.US_ASCII))
              out.flush()
            } finally {
              try sslSocket.close()
              catch { case _: Throwable => () }
              try rawSocket.close()
              catch { case _: Throwable => () }
            }
            // Full re-request on a fresh connection is byte-exact, plus a
            // clean follow-up on yet another connection.
            val first     = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
            val second    = TlsAlpnFixtures.rawTlsH1Get(port, Array("http/1.1"), "/ok")
            (first, second)
          }.map { case ((n1, s1, b1), (n2, s2, b2)) =>
            assertTrue(n1 == "http/1.1", s1 == 200, b1 == BodyText, n2 == "http/1.1", s2 == 200, b2 == BodyText)
          }
        }
      },
      test("dispatch refuses unknown offer lists before bind") {
        ZIO.attemptBlocking {
          val connector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig(List("h2", "spdy/3"))),
          )
          val engine    = new H1H2TlsEngine(okRoutes, Context.empty, connector, DefectHandler.default)
          val caught    =
            try {
              engine.start()
              None
            } catch {
              case error: InvalidConnector => Some(error)
            }
          assertTrue(caught.exists(_.failure == ConnectorFailure.UnknownAlpnProtocol("spdy/3")))
        }
      },
    ) @@ sequential
}

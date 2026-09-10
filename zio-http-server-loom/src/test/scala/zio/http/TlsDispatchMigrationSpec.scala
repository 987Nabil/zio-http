package zio.http

import scala.annotation.experimental

import zio._
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.http.h1.H1RawClientFixture.RawH1Client
import zio.http.h1.H1Transport
import zio.http.h2.H2Transport
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 11 migration fixture: legacy entry points behave exactly as before, the
 * dispatch engine wires through `LoomServer`, and invalid registrations fail
 * fast with typed errors before any socket binds.
 */
@experimental
object TlsDispatchMigrationSpec extends ZIOSpecDefault {

  private val BodyText = "tls-migration-ok"

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      Handler.succeed(Response.text(BodyText)),
    ),
  )

  private def tcpPortOf(handle: ServerHandle): Int =
    handle.bindings.head.address match {
      case BoundAddress.Tcp(_, value) => value
      case other                      => throw new AssertionError("Expected TCP binding: " + other)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("TlsDispatchMigrationSpec")(
      test("legacy direct H2Transport over StrictH2 TLS still serves H2") {
        ZIO
          .acquireRelease(
            ZIO.attempt {
              val connector = Connector(
                bind = BindAddress.localhost(0),
                protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig(List("h2"), AlpnPolicy.StrictH2)),
              )
              new H2Transport(okRoutes, Context.empty, connector, DefectHandler.default).start()
            },
          )(handle => ZIO.succeed(handle.close0()))
          .flatMap { handle =>
            val port = handle.binding.address match {
              case BoundAddress.Tcp(_, value) => value
              case other                      => throw new AssertionError("Expected TCP: " + other)
            }
            ZIO.attemptBlocking {
              val client            = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
              val (_, status, body) = TlsAlpnFixtures.jdkGet(client, port, "/ok")
              (status, body)
            }.map { case (status, body) =>
              assertTrue(status == 200, body == BodyText)
            }
          }
      },
      test("legacy LoomServer without engines still serves H2 and rejects H1") {
        ZIO
          .acquireRelease(
            ZIO.attempt {
              val connector = Connector(
                bind = BindAddress.localhost(0),
                protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig(List("h2"), AlpnPolicy.StrictH2)),
              )
              LoomServer(connector).serve(okRoutes, Context.empty)
            },
          )(handle => ZIO.succeed(handle.shutdownAndWait()))
          .flatMap { handle =>
            ZIO.attemptBlocking(TlsAlpnFixtures.handshakeOnly(tcpPortOf(handle), Array("http/1.1"))).exit.flatMap {
              exit =>
                val rejectedAtTls = exit match {
                  case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[javax.net.ssl.SSLException])
                  case _                   => false
                }
                ZIO.attemptBlocking {
                  val client            = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
                  val (_, status, body) = TlsAlpnFixtures.jdkGet(client, tcpPortOf(handle), "/ok")
                  (rejectedAtTls, exit.isFailure, status, body)
                }.map { case (rejectedAtTls, failed, status, body) =>
                  assertTrue(rejectedAtTls, failed, status == 200, body == BodyText)
                }
            }
          }
      },
      test("legacy cleartext H1Transport still serves H1") {
        ZIO
          .acquireRelease(
            ZIO.attempt {
              val connector = Connector(bind = BindAddress.localhost(0))
              new H1Transport(okRoutes, Context.empty, connector, DefectHandler.default).start()
            },
          )(handle => ZIO.succeed(handle.close0()))
          .flatMap { handle =>
            val port = handle.binding.address match {
              case BoundAddress.Tcp(_, value) => value
              case other                      => throw new AssertionError("Expected TCP: " + other)
            }
            ZIO.attemptBlocking {
              val client = new RawH1Client(port)
              try {
                client.sendRaw("GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                val response = client.readResponse()
                (response.status, response.bodyText)
              } finally client.close()
            }.map { case (status, body) =>
              assertTrue(status == 200, body == BodyText)
            }
          }
      },
      test("dispatch engine through LoomServer serves H1 and H2 on one port") {
        ZIO
          .acquireRelease(
            ZIO.attempt {
              val connector =
                Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig()))
              val engine    = new H1H2TlsEngine(okRoutes, Context.empty, connector, DefectHandler.default)
              require(engine.connector.eq(connector), "dispatch engine owns its connector")
              LoomServer(connector).withEngine(engine).serve(okRoutes, Context.empty)
            },
          )(handle => ZIO.succeed(handle.shutdownAndWait()))
          .flatMap { handle =>
            ZIO.attemptBlocking {
              val h2Client              = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
              val (_, h2Status, h2Body) = TlsAlpnFixtures.jdkGet(h2Client, tcpPortOf(handle), "/ok")
              val (h1Negotiated, h1Status, h1Body) =
                TlsAlpnFixtures.rawTlsH1Get(tcpPortOf(handle), Array("http/1.1"), "/ok")
              (h2Status, h2Body, h1Negotiated, h1Status, h1Body)
            }.map { case (h2Status, h2Body, h1Negotiated, h1Status, h1Body) =>
              assertTrue(
                h2Status == 200,
                h2Body == BodyText,
                h1Negotiated == "http/1.1",
                h1Status == 200,
                h1Body == BodyText,
              )
            }
          }
      },
      test("dispatch plus standalone H1 fails fast with DuplicateProtocol before bind") {
        ZIO.attemptBlocking {
          val tlsConnector =
            Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig()))
          val h1Connector  = Connector(bind = BindAddress.localhost(0))
          val dispatch     = new H1H2TlsEngine(okRoutes, Context.empty, tlsConnector, DefectHandler.default)
          val h1           = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val caught       =
            try {
              LoomServer(tlsConnector)
                .addConnector(h1Connector)
                .withEngines(List(dispatch, h1))
                .serve(okRoutes, Context.empty)
              None
            } catch {
              case error: EngineRegistrationError.DuplicateProtocol => Some(error)
            }
          assertTrue(caught.isDefined, caught.exists(_.protocol == ProtocolId.Http1))
        }
      },
      test("dispatch engine on a cleartext connector fails fast typed before bind") {
        ZIO.attemptBlocking {
          val connector = Connector(bind = BindAddress.localhost(0))
          val engine    = new H1H2TlsEngine(okRoutes, Context.empty, connector, DefectHandler.default)
          val caught    =
            try {
              LoomServer(connector).withEngine(engine).serve(okRoutes, Context.empty)
              None
            } catch {
              case error: InvalidConnector => Some(error)
            }
          assertTrue(caught.isDefined)
        }
      },
    ) @@ sequential
}

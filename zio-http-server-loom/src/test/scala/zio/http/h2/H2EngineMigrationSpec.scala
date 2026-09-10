package zio.http.h2

import java.nio.file.Paths

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.h2.H2Frame._
import zio.http.h2.H2RawClientFixture.RawH2Client
import zio.http.{
  BindAddress,
  BoundAddress,
  Connector,
  DefectHandler,
  EngineId,
  EngineRegistrationError,
  Handler,
  LoomServer,
  Protocol,
  ProtocolId,
  Response,
  Route,
  Routes,
  Server,
  TlsConfig,
  TlsSource,
  TransportKind,
}

/**
 * Todo 6 migration fixture: moving H2 behind [[H2Engine]].
 *
 * Intentional pre-release source notes recorded here:
 *
 *   - `new H2Transport(routes, context, connector, defectHandler).start()`
 *     keeps serving exactly as before (entry point preserved; the pre-existing
 *     raw-wire suite pins the behavior).
 *   - `LoomServer(connector)` without engines keeps serving H2C exactly as
 *     before: serving now flows through an internally-created [[H2Engine]]
 *     instead of a bare transport (source-compatible: `serve` is unchanged).
 *   - Explicit registration `LoomServer(connector).withEngine(h2Engine)` runs
 *     the registered engine instance when its connector matches; other engine
 *     implementations still validate-then-serve as in Todo 1.
 *   - `serve` still validates registered engines BEFORE any socket is bound and
 *     throws a deterministic [[EngineRegistrationError]].
 */
@experimental
object H2EngineMigrationSpec extends ZIOSpecDefault {

  private val TestRoutes: Routes[Any] =
    Routes(Route(RoutePattern.GET, Handler.succeed(Response.text("h2-migration-ok"))))

  private def roundTrip(port: Int, streamId: Int): (Int, String) = {
    val client = new RawH2Client(port)
    try {
      val response = client.roundTrip("GET", "/", Chunk.empty, streamId)
      (response.status, response.bodyText)
    } finally client.close()
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H2EngineMigrationSpec")(
      test("legacy H2Transport construction still serves (pre-Todo-6 entry point preserved)") {
        ZIO.attemptBlocking {
          val handle = zio.http.ServerHandle.live(
            List(
              new H2Transport(
                TestRoutes,
                Context.empty,
                Connector(bind = BindAddress.localhost(0)),
                DefectHandler.default,
              )
                .start(),
            ),
          )
          try {
            val port           = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding: " + other)
            }
            val (status, body) = roundTrip(port, 1)
            assertTrue(status == 200, body == "h2-migration-ok")
          } finally handle.shutdownAndWait()
        }
      },
      test("loom server without engines serves H2C exactly as before") {
        ZIO.attemptBlocking {
          val server = LoomServer(Connector(bind = BindAddress.localhost(0)))
          val handle = Server.serve(TestRoutes, Context.empty.add(server))
          try {
            val port           = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding: " + other)
            }
            val (status, body) = roundTrip(port, 1)
            assertTrue(handle.bindings.length == 1, status == 200, body == "h2-migration-ok")
          } finally handle.shutdownAndWait()
        }
      },
      test("registered H2 engine instance serves and its drain is observable on the wire") {
        ZIO.attemptBlocking {
          val connector = Connector(bind = BindAddress.localhost(0))
          val engine    = new H2Engine(TestRoutes, Context.empty, connector, DefectHandler.default, EngineId("h2-test"))
          val server    = LoomServer(connector).withEngine(engine)
          val handle    = Server.serve(TestRoutes, Context.empty.add(server))
          try {
            val port   = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding: " + other)
            }
            // Same engine identity the registry validated...
            assertTrue(engine.id == EngineId("h2-test"), engine.transportKind == TransportKind.Tcp)
            // ...and its drain acts on the live serving connection: the
            // registered instance is started, not a copy. The drain's
            // GOAWAY arrives on the pre-drain connection (drain never stops
            // the listener from accepting new TCP connections — that is the
            // aggregate lifecycle's job in a later todo).
            val client = new RawH2Client(port)
            try {
              val response = client.roundTrip("GET", "/", Chunk.empty, 1)
              engine.drain()
              val drained  = client.readNextMeaningfulFrame() match {
                case GoAway(_, code, _) => code == H2Error.Code.NO_ERROR
                case _                  => false
              }
              assertTrue(response.status == 200, response.bodyText == "h2-migration-ok", drained)
            } finally client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("duplicate H2 protocol registration still fails serve fast with a typed error") {
        ZIO.attemptBlocking {
          val connector = Connector(bind = BindAddress.localhost(0))
          val first     =
            new H2Engine(TestRoutes, Context.empty, connector, DefectHandler.default, EngineId("h2-a"))
          val second    =
            new H2Engine(TestRoutes, Context.empty, connector, DefectHandler.default, EngineId("h2-b"))
          val server    = LoomServer(connector).withEngines(List(first, second))
          val result    =
            try {
              val handle = Server.serve(TestRoutes, Context.empty.add(server))
              try Left("bound")
              finally handle.shutdownAndWait()
            } catch {
              case error: EngineRegistrationError.DuplicateProtocol => Right(error)
            }
          assertTrue(
            result == Right(
              EngineRegistrationError.DuplicateProtocol(ProtocolId.H2C, EngineId("h2-a"), EngineId("h2-b")),
            ),
          )
        }
      },
      test("h3 connector protocol mapping fails fast (no engine yet)") {
        ZIO.attempt {
          val connector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H3(
              TlsConfig(TlsSource.FilePath(Paths.get("cert.pem")), TlsSource.FilePath(Paths.get("key.pem"))),
            ),
          )
          val result    =
            try {
              H2Engine.protocolsFor(connector)
              None
            } catch {
              case error: UnsupportedOperationException => Some(error.getMessage)
            }
          assertTrue(result.exists(_.contains("H3/QUIC")))
        }
      },
    ) @@ sequential
}

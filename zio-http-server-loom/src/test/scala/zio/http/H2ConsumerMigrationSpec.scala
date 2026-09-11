package zio.http

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.h1._
import zio.http.h2.H2Engine
import zio.http.h2.H2RawClientFixture.RawH2Client

/**
 * Todo 19 consumer migration fixture: integrating the multi-protocol Loom
 * artifacts (publishing, cross-builds, compatibility).
 *
 * Migration notes (exact, no Scala 3 API breaks in Todo 19 — this todo is
 * build-only: one workflow line plus this fixture):
 *
 *   - `zio-http-h1-codec` is published as a pure leaf: its only dependency is
 *     `zio-blocks-chunk`. It never depends on core/server/Loom, and nothing may
 *     add such an edge (see `H1CodecPublishContractSpec` for the
 *     dependency-direction pins).
 *   - Engines stay in `zio-http-server-loom` (`H1Transport`, `H2Engine`, the
 *     TLS/cleartext dispatch engines). No new engine module is introduced
 *     unless dependency evidence justifies it — none does.
 *   - Consumer Mill wiring: serving needs `serverLoom` alone (it already
 *     depends on `h1Codec`/`h2Codec` transitively); direct codec use adds the
 *     leaf explicitly:
 *     {{{
 *     def moduleDeps = Seq(serverLoom.jvm(), h1Codec.jvm())
 *     }}}
 *   - Snapshot publishing (`snapshot.yml`) ships
 *     `{core,server,client,endpoint,h1Codec,h2Codec,serverLoom,clientJava,zio,testkit}`
 *     for every cross Scala version (2.13.18 retained, 3.9.0 authoritative).
 *   - The H2-only consumer shape `LoomServer(connector)` is unchanged
 *     (source-compatible: engines default to `Nil`, serving flows through an
 *     internally-created [[H2Engine]]). Explicit multi-protocol registration
 *     uses `withEngine`/`withEngines` with deterministic typed failures.
 *   - Scala 2.13 facades are owned by Todo 22; this fixture compiles on both
 *     toolchains without constraining the Scala 3 API.
 */
@experimental
object H2ConsumerMigrationSpec extends ZIOSpecDefault {

  private val ConsumerRoutes: Routes[Any] =
    Routes(Route(RoutePattern.GET, Handler.succeed(Response.text("consumer-ok"))))

  private def h2cRoundTrip(port: Int): (Int, String) = {
    val client = new RawH2Client(port)
    try {
      val response = client.roundTrip("GET", "/", Chunk.empty, 1)
      (response.status, response.bodyText)
    } finally client.close()
  }

  private def tcpPort(handle: ServerHandle): Int =
    handle.bindings.head.address match {
      case BoundAddress.Tcp(_, port) => port
      case other                     => throw new AssertionError("Expected TCP binding: " + other)
    }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H2ConsumerMigrationSpec")(
      test("h1Codec leaf decodes for a direct consumer without the server") {
        ZIO.attempt {
          val decoder = new H1Decoder()
          val result  =
            decoder.feed(Chunk.fromArray("GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n".getBytes("US-ASCII")))
          assertTrue(
            result == Right(
              List(
                H1Request(
                  method = "GET",
                  target = "/hello",
                  headers = H1Headers(List(H1Header("Host", "example.com"))),
                  body = Chunk.empty,
                  framing = H1BodyFraming.Empty,
                  trailers = H1Headers.Empty,
                ),
              ),
            ),
          )
        }
      },
      test("legacy H2C protocol maps to the h2cOnly protocol set") {
        ZIO.attempt {
          val connector = Connector(bind = BindAddress.localhost(0))
          assertTrue(
            ProtocolSet.fromLegacy(connector.protocol) == Right(ProtocolSet.h2cOnly),
            H2Engine.protocolsFor(connector) == Set[ProtocolId](ProtocolId.H2C),
          )
        }
      },
      test("h2-only consumer construction keeps serving through serverLoom") {
        ZIO.attemptBlocking {
          val server = LoomServer(Connector(bind = BindAddress.localhost(0)))
          val handle = Server.serve(ConsumerRoutes, Context.empty.add(server))
          try {
            val (status, body) = h2cRoundTrip(tcpPort(handle))
            assertTrue(handle.bindings.length == 1, status == 200, body == "consumer-ok")
          } finally handle.shutdownAndWait()
        }
      },
      test("registered H2 engine serves the same consumer routes") {
        ZIO.attemptBlocking {
          val connector = Connector(bind = BindAddress.localhost(0))
          val engine    =
            new H2Engine(ConsumerRoutes, Context.empty, connector, DefectHandler.default, EngineId("h2-consumer"))
          val server    = LoomServer(connector).withEngine(engine)
          val handle    = Server.serve(ConsumerRoutes, Context.empty.add(server))
          try {
            val (status, body) = h2cRoundTrip(tcpPort(handle))
            assertTrue(
              engine.id == EngineId("h2-consumer"),
              engine.transportKind == TransportKind.Tcp,
              status == 200,
              body == "consumer-ok",
            )
          } finally handle.shutdownAndWait()
        }
      },
    ) @@ sequential
}

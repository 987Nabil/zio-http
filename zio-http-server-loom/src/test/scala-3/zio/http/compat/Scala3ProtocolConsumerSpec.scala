package zio.http.compat

import java.io.ByteArrayOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets

import scala.annotation.experimental

import zio._
import zio.blocks.config.Secret
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test._

import zio.http._
import zio.http.ResultType._
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine

/**
 * Todo 22: Scala 3 mirror of the 2.13 consumer fixture.
 *
 * Pins identical semantics through the Scala 3 facades with idiomatic Scala 3
 * syntax (inferred `Set`/`List` element types saved by declared result types,
 * `new`-free construction). Any divergence from
 * [[Scala213ProtocolConsumerSpec]] is a compatibility bug, not a dialect
 * difference.
 */
@experimental
object Scala3ProtocolConsumerSpec extends ZIOSpecDefault {

  private val pingRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern.GET,
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("pong")))
      },
    ),
  )

  private val ephemeral: Connector =
    Connector(bind = BindAddress.localhost(0))

  private val h1Engine: H1Transport[Any] =
    Scala3EngineCompat.h1Engine[Any](pingRoutes, Context.empty, ephemeral, DefectHandler.default)

  private def readAll(socket: Socket): Array[Byte] = {
    val in  = socket.getInputStream
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](4096)
    var n   = in.read(buf)
    while (n >= 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    out.toByteArray
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Scala3ProtocolConsumerSpec")(
      test("builds a single-H1 registry through the facade") {
        val built = Scala3ProtocolCompat.singleEngineRegistry(h1Engine)
        assertTrue(
          built.isRight,
          built.toOption.map(_.protocols) == Some(Scala3ProtocolCompat.h1Protocols),
          built.toOption.flatMap(_.engineFor(ProtocolId.Http1)).isDefined,
        )
      },
      test("facade protocol sets match the shared constants") {
        assertTrue(
          Scala3ProtocolCompat.h1Protocols == Set(ProtocolId.Http1),
          Scala3ProtocolCompat.h2cProtocols == Set(ProtocolId.H2C),
          Scala3ProtocolCompat.h1H2cProtocols == Set(ProtocolId.Http1, ProtocolId.H2C),
          Scala3ProtocolCompat.protocolSetOf(Seq(AppProtocol.Http1, AppProtocol.H2C)) ==
            Right(ProtocolSet.h1h2c),
        )
      },
      test("rejects duplicate protocols deterministically") {
        val first: H2Engine[Any]  =
          Scala3EngineCompat.h2Engine[Any](pingRoutes, Context.empty, ephemeral, DefectHandler.default)
        val second: H2Engine[Any] = H2Engine[Any](
          pingRoutes,
          Context.empty,
          ephemeral,
          DefectHandler.default,
          EngineId("h2-second"),
        )
        val built                 = Scala3ProtocolCompat.registryOf(List(first, second))
        assertTrue(built match {
          case Left(EngineRegistrationError.DuplicateProtocol(ProtocolId.H2C, _, _)) => true
          case _                                                                     => false
        })
      },
      test("rejects empty and incompatible registries") {
        val empty                      = Scala3ProtocolCompat.registryOf(List.empty)
        val unixEngine: ProtocolEngine = new ProtocolEngine {
          def id: EngineId                        = EngineId("unix-future")
          def transportKind: TransportKind        = TransportKind.Unix
          def supportedProtocols: Set[ProtocolId] = Set(ProtocolId.Http1)
          def drain(): Unit                       = ()
          def close(): Unit                       = ()
        }
        val mixed                      = Scala3ProtocolCompat.registryOf(List(h1Engine, unixEngine))
        assertTrue(
          empty == Left(EngineRegistrationError.EmptyRegistry),
          mixed match {
            case Left(EngineRegistrationError.IncompatibleTransport(_, _, _)) => true
            case _                                                            => false
          },
        )
      },
      test("reports duplicate and empty protocol sets") {
        assertTrue(
          Scala3ProtocolCompat.protocolSetOf(
            Seq(AppProtocol.Http1, AppProtocol.Http1),
          ) == Left(ConnectorFailure.DuplicateProtocol(AppProtocol.Http1)),
          Scala3ProtocolCompat.protocolSetOf(Seq.empty) ==
            Left(ConnectorFailure.EmptyProtocolSet),
        )
      },
      test("H3 has no engine mapping on Scala 3 either") {
        val h3 = Protocol.H3(
          TlsConfig(
            TlsSource.PemString(Secret("todo22-fixture-not-a-cert")),
            TlsSource.PemString(Secret("todo22-fixture-not-a-key")),
          ),
          QuicConfig(),
          Http3Config(),
        )
        assertTrue(ProtocolSet.fromLegacy(h3) == Left(ConnectorFailure.H3NotAdvertised))
      },
      test("facade-built H2 engine claims H2C on a cleartext connector") {
        val h2c: H2Engine[Any] =
          Scala3EngineCompat.h2Engine[Any](pingRoutes, Context.empty, ephemeral, DefectHandler.default)
        assertTrue(
          h2c.supportedProtocols == Scala3ProtocolCompat.h2cProtocols,
          h2c.transportKind == TransportKind.Tcp,
        )
      },
      test("serves an H1 loopback through the facade-built engine") {
        ZIO
          .acquireRelease(
            ZIO.attempt(LoomServer(ephemeral).withEngine(h1Engine).serve(pingRoutes, Context.empty)),
          )(handle => ZIO.succeed(handle.shutdownAndWait()))
          .flatMap { handle =>
            val port: Int = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding but found: " + other)
            }
            ZIO.attemptBlocking {
              val socket = new Socket("127.0.0.1", port)
              socket.setSoTimeout(10000)
              try {
                val out  = socket.getOutputStream
                out.write(
                  "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.US_ASCII),
                )
                out.flush()
                val text = new String(readAll(socket), StandardCharsets.UTF_8)
                assertTrue(text.startsWith("HTTP/1.1 200"), text.contains("pong"))
              } finally socket.close()
            }
          }
      },
    )
}

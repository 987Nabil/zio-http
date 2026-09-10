package zio.http.h2

import java.io.EOFException
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.h2.H2Frame._
import zio.http.h2.H2RawClientFixture.RawH2Client
import zio.http.h2.hpack.HeaderField
import zio.http.{
  BindAddress,
  BoundAddress,
  Connector,
  DefectHandler,
  EngineId,
  Handler,
  Http2Config,
  LoomServer,
  Protocol,
  ProtocolId,
  Response,
  Route,
  Routes,
  Server,
  TransportKind,
}

/**
 * Todo 6: the H2 transport behind the [[ProtocolEngine]] contract.
 *
 * The H2 wire stack (H2Transport / H2Connection / H2ConnectionControl /
 * FlowController) is owned by [[H2Engine]]: `LoomServer.serve` runs H2 through
 * the engine, `drain` maps to GOAWAY(NO_ERROR) plus waking blocked flow/frame
 * waiters, and `close` force-releases them. #4291 (body cap, deadlines, proxy
 * trust, access log) and #4292 (settings/ALPN/timeouts/streaming/SSE) behavior
 * is preserved: every pre-existing H2 suite must stay green unchanged.
 */
@experimental
object H2EngineSpec extends ZIOSpecDefault {

  private val TestRoutes: Routes[Any] =
    Routes(Route(RoutePattern.GET, Handler.succeed(Response.text("h2-engine-ok"))))

  private def h2Engine(connector: Connector): H2Engine[Any] =
    new H2Engine(TestRoutes, Context.empty, connector, DefectHandler.default)

  private def serveThrough(engine: H2Engine[Any]): (zio.http.ServerHandle, Int) = {
    val server  = LoomServer(engine.connector).withEngine(engine)
    val context = Context.empty.add(server)
    val handle  = Server.serve(TestRoutes, context)
    val port    = handle.bindings.head.address match {
      case BoundAddress.Tcp(_, p) => p
      case other                  => throw new AssertionError("Expected TCP binding: " + other)
    }
    (handle, port)
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H2EngineSpec")(
      test("h2c engine reports Tcp transport and the H2C protocol") {
        ZIO.attempt {
          val engine = h2Engine(Connector(bind = BindAddress.localhost(0)))
          assertTrue(
            engine.transportKind == TransportKind.Tcp,
            engine.supportedProtocols == Set[ProtocolId](ProtocolId.H2C),
          )
        }
      },
      test("engine identity defaults to h2 and is overridable") {
        ZIO.attempt {
          val defaultId = h2Engine(Connector(bind = BindAddress.localhost(0))).id
          val custom    =
            new H2Engine(
              TestRoutes,
              Context.empty,
              Connector(bind = BindAddress.localhost(0)),
              DefectHandler.default,
              EngineId("h2-test"),
            ).id
          assertTrue(defaultId == EngineId("h2"), custom == EngineId("h2-test"))
        }
      },
      test("loom server serves requests through a registered H2 engine") {
        ZIO.attemptBlocking {
          val engine         = h2Engine(Connector(bind = BindAddress.localhost(0)))
          val (handle, port) = serveThrough(engine)
          try {
            val client = new RawH2Client(port)
            try {
              val response = client.roundTrip("GET", "/", Chunk.empty, streamId = 1)
              assertTrue(response.status == 200, response.bodyText == "h2-engine-ok")
            } finally client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("engine drain emits GOAWAY(NO_ERROR) with a real lastStreamId on the wire") {
        ZIO.attemptBlocking {
          val engine         = h2Engine(Connector(bind = BindAddress.localhost(0)))
          val (handle, port) = serveThrough(engine)
          try {
            val client = new RawH2Client(port)
            try {
              val first  = client.roundTrip("GET", "/", Chunk.empty, streamId = 1)
              engine.drain()
              val goAway = awaitGoAway(client, 10000L)
              assertTrue(
                first.status == 200,
                goAway != null,
                goAway.errorCode == H2Error.Code.NO_ERROR,
                goAway.lastStreamId == 1,
              )
            } finally client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("in-flight streams complete through a drain while new streams are refused") {
        ZIO.attemptBlocking {
          val engine         = h2Engine(Connector(bind = BindAddress.localhost(0)))
          val (handle, port) = serveThrough(engine)
          try {
            val client = new RawH2Client(port)
            try {
              // Stream 1 completes before the drain (advances highestStreamId).
              val firstOk = client.roundTrip("GET", "/", Chunk.empty, streamId = 1).status == 200
              // Stream 3 opens without END_STREAM: its handler parks in the
              // body read until the client finishes the body. The settle
              // delay lets the server reader deliver the HEADERS (opening
              // the stream and advancing highestStreamId) before the drain:
              // without it the drain's GOAWAY could precede the HEADERS and
              // the stream would be refused instead of in-flight.
              client.sendFrame(headersFrame(client, 3, endStream = false))
              Thread.sleep(1500)
              engine.drain()
              // The parked frame waiter survives the drain: finishing the body
              // still yields the full response (drain wakes, never aborts).
              client.sendFrame(Data(3, Chunk.empty, endStream = true))
              val status  = awaitStatusToleratingGoAway(client, 3, 10000L)
              // A stream opened after our GOAWAY is refused so the peer can
              // retry elsewhere (RFC 9113 section 6.8).
              client.sendFrame(headersFrame(client, 5, endStream = true))
              val refused = awaitRst(client, 5, 10000L)
              assertTrue(firstOk, status == 200, refused.contains(H2Error.Code.REFUSED_STREAM))
            } finally client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("engine close releases a sender blocked on an exhausted flow-control window") {
        ZIO.attemptBlocking {
          // Stream window 0 with no client WINDOW_UPDATE: the response DATA
          // send parks in consumeSendWindow until the 30s bound. Close must
          // release it promptly instead of parking to the deadline.
          val bigBody   = "x" * 512
          val routes    = Routes(Route(RoutePattern.GET, Handler.succeed(Response.text(bigBody))))
          val connector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2C(Http2Config(initialWindowSize = 0)),
          )
          val engine    = new H2Engine(routes, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          val handle    = Server.serve(routes, Context.empty.add(server))
          try {
            val port   = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding: " + other)
            }
            val client = new RawH2Client(port)
            try {
              client.sendFrame(headersFrame(client, 1, endStream = true))
              // Let the handler park in the flow-control wait.
              Thread.sleep(1500)
              val start   = java.lang.System.nanoTime()
              engine.close()
              handle.shutdown()
              val elapsed = java.lang.System.nanoTime() - start
              // Pre-Todo-6 code parks to the 30s flow-control deadline here;
              // the drain-mapped wake must release it an order of magnitude
              // faster (15s bound keeps loaded CI green either way).
              assertTrue(elapsed < 15000000000L)
            } finally client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("flow control invalidated by close fails parked senders fast") {
        ZIO.attemptBlocking {
          val flow    = new FlowController(100, 0)
          flow.registerStream(1)
          val outcome = new AtomicReference[String]("waiting")
          val waiter  = Thread
            .ofVirtual()
            .start(() => {
              try {
                flow.consumeSendWindow(1, 50, 60000L)
                outcome.set("completed")
              } catch {
                case _: IllegalStateException => outcome.set("released")
                case _: Throwable             => outcome.set("other")
              }
            })
          // Let the waiter park on the empty stream window.
          Thread.sleep(1000)
          val start   = java.lang.System.nanoTime()
          flow.invalidate()
          waiter.join(10000)
          val elapsed = java.lang.System.nanoTime() - start
          assertTrue(!waiter.isAlive, outcome.get() == "released", elapsed < 10000000000L)
        }
      },
      test("flow control drain wake does not break a subsequent window grant") {
        ZIO.attemptBlocking {
          val flow         = new FlowController(100, 0)
          flow.registerStream(1)
          val outcome      = new AtomicReference[String]("waiting")
          val waiter       = Thread
            .ofVirtual()
            .start(() => {
              try {
                flow.consumeSendWindow(1, 50, 60000L)
                outcome.set("completed")
              } catch {
                case _: Throwable => outcome.set("failed")
              }
            })
          Thread.sleep(1000)
          // Drain wakes the waiter; with no new window it re-parks and still
          // completes once the grant arrives (drain never aborts in-flight).
          flow.wakeAll()
          Thread.sleep(500)
          val stillWaiting = outcome.get() == "waiting" && waiter.isAlive
          flow.applyWindowUpdate(1, 100)
          waiter.join(10000)
          assertTrue(stillWaiting, !waiter.isAlive, outcome.get() == "completed")
        }
      },
    ) @@ sequential

  // ─── raw-wire helpers ─────────────────────────────────────────────────────

  private def headersFrame(client: RawH2Client, streamId: Int, endStream: Boolean): Headers =
    Headers(
      streamId = streamId,
      headerBlock = client.encodeHeaders(
        List(
          HeaderField(":method", "GET"),
          HeaderField(":path", "/"),
          HeaderField(":scheme", "http"),
          HeaderField(":authority", "127.0.0.1"),
        ),
      ),
      endStream = endStream,
      endHeaders = true,
    )

  private def awaitGoAway(client: RawH2Client, timeoutMs: Long): GoAway = {
    val deadline       = java.lang.System.currentTimeMillis() + timeoutMs
    var result: GoAway = null
    while (result == null && java.lang.System.currentTimeMillis() < deadline) {
      client.socket.setSoTimeout(Math.max(1, (deadline - java.lang.System.currentTimeMillis()).toInt))
      try {
        client.readFrame() match {
          case s: Settings     => if (!s.ack) client.sendFrame(Settings(ack = true, Nil))
          case _: WindowUpdate => ()
          case g: GoAway       => result = g
          case _               => ()
        }
      } catch {
        case _: java.net.SocketTimeoutException => ()
        case _: EOFException                    => return result
      }
    }
    result
  }

  private def awaitStatusToleratingGoAway(client: RawH2Client, streamId: Int, timeoutMs: Long): Int = {
    val deadline = java.lang.System.currentTimeMillis() + timeoutMs
    var status   = -1
    var done     = false
    while (!done && java.lang.System.currentTimeMillis() < deadline) {
      client.socket.setSoTimeout(Math.max(1, (deadline - java.lang.System.currentTimeMillis()).toInt))
      try {
        client.readFrame() match {
          case s: Settings     => if (!s.ack) client.sendFrame(Settings(ack = true, Nil))
          case _: WindowUpdate => ()
          case _: GoAway       => ()
          case Headers(sid, block, end, _, _, _) if sid == streamId =>
            client.decodeHeaders(block).find(_.name == ":status").foreach(f => status = f.value.toInt)
            done = end
          case Data(sid, _, end, _) if sid == streamId              => done = end
          case RstStream(sid, code) if sid == streamId              =>
            throw new AssertionError("Unexpected RST_STREAM: " + code)
          case _                                                    => ()
        }
      } catch {
        case _: java.net.SocketTimeoutException => ()
      }
    }
    status
  }

  private def awaitRst(client: RawH2Client, streamId: Int, timeoutMs: Long): Option[H2Error.Code] = {
    val deadline                     = java.lang.System.currentTimeMillis() + timeoutMs
    var result: Option[H2Error.Code] = None
    while (result.isEmpty && java.lang.System.currentTimeMillis() < deadline) {
      client.socket.setSoTimeout(Math.max(1, (deadline - java.lang.System.currentTimeMillis()).toInt))
      try {
        client.readFrame() match {
          case s: Settings                             => if (!s.ack) client.sendFrame(Settings(ack = true, Nil))
          case _: WindowUpdate                         => ()
          case _: GoAway                               => ()
          case RstStream(sid, code) if sid == streamId => result = Some(code)
          case _                                       => ()
        }
      } catch {
        case _: java.net.SocketTimeoutException => ()
        case _: EOFException                    => return result
      }
    }
    result
  }
}

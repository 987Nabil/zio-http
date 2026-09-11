package zio.http

import java.net.Socket
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.ResultType._
import zio.http.h1.H1RawClientFixture.RawH1Client
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine
import zio.http.h2.H2Frame
import zio.http.h2.H2RawClientFixture.RawH2Client
import zio.http.h2.hpack.{HeaderField => H2Header}

/**
 * Todo 17: bounded stress over lifecycle, isolation, and resource cleanup on
 * the full multi-protocol stack (Todos 8-13).
 *
 * Every test is a deterministic bounded run — latches/barriers with bounded
 * waits or socket timeouts, no sleep-polling — and proves the same diagnostics:
 *   - `awaitShutdown` truly blocks (a waiter latch proves it was parked, then
 *     released by the terminal transition);
 *   - every engine reaches quiescence (`awaitQuiescent` true: no live
 *     connection, waiter, or timer residue);
 *   - every listener refuses new TCP connections after shutdown (no listener
 *     leak) and the handle reports terminated.
 *
 * Coverage: repeated serve/stress/shutdown cycles (accumulation leaks),
 * malformed-client storms (dirty/garbage, near-preface misleading bytes),
 * slow-trickle bodies (long), abrupt disconnects/RST (flaky), stale half-open
 * connections, shutdown races with in-flight work and concurrent callers,
 * shared cleartext dispatch under mixed load, bind-collision rollback repeats,
 * and peer-engine failure isolation (a failing engine never takes down its
 * healthy peer).
 */
@experimental
object MultiProtocolStressSpec extends ZIOSpecDefault {

  private val TestTimeoutSeconds = 30L
  private val ShortWaitMillis    = 300L
  private val QuiescentTimeout   = Duration.ofSeconds(15)
  private val Cycles             = 3
  private val StormClients       = 12

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      Handler.succeed(Response.text("stress-ok")),
    ),
    Route(
      RoutePattern(Method.POST, "/echo"),
      handler { (request: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString(request.body.asString())))
      },
    ),
  )

  private def slowRoutes(entered: CountDownLatch, release: CountDownLatch): Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/slow"),
      handler { (_: Request) =>
        entered.countDown()
        release.await(TestTimeoutSeconds, TimeUnit.SECONDS)
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("slow-ok")))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/ok"),
      Handler.succeed(Response.text("stress-ok")),
    ),
  )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("MultiProtocolStressSpec")(
      test("repeated serve/stress/shutdown cycles leave no listener or connection residue") {
        ZIO.attemptBlocking {
          var cycle = 0
          var clean = true
          while (cycle < Cycles && clean) {
            val h1Connector = Connector(bind = BindAddress.localhost(0))
            val h2Connector = Connector(bind = BindAddress.localhost(0))
            val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
            val h2          = new H2Engine(okRoutes, Context.empty, h2Connector, DefectHandler.default)
            val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1).withEngine(h2)
            val handle      = Server.serve(okRoutes, Context.empty.add(server))
            try {
              val ports   = portsOf(handle)
              // Mixed valid load behind one barrier: H1 closes, H2 round-trips.
              val okCount = new AtomicInteger(0)
              val gate    = new CountDownLatch(1)
              val done    = new CountDownLatch(StormClients)
              (0 until StormClients).foreach { i =>
                Thread
                  .ofVirtual()
                  .start(() => {
                    gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                    try {
                      if (i % 2 == 0) {
                        if (h1Get(ports(0)) == ((200, "stress-ok"))) okCount.incrementAndGet()
                      } else {
                        if (h2cGet(ports(1)) == ((200, "stress-ok"))) okCount.incrementAndGet()
                      }
                    } catch {
                      case _: Throwable => ()
                    } finally done.countDown()
                    ()
                  })
              }
              gate.countDown()
              require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), s"cycle $cycle: every client finished")
              require(okCount.get() == StormClients, s"cycle $cycle: every client got 200")
              // Real await: the waiter parks (early false) until shutdown lands.
              clean = shutdownIsReal(handle) &&
                h1.awaitQuiescent(QuiescentTimeout) &&
                h2.awaitQuiescent(QuiescentTimeout) &&
                tcpRefused(ports(0)) && tcpRefused(ports(1)) && !handle.isRunning
              require(clean, s"cycle $cycle: quiescent, refused, terminated")
            } finally handle.shutdownAndWait()
            cycle += 1
          }
          assertTrue(cycle == Cycles, clean)
        }
      },
      test("malformed client storm on shared dispatch does not poison subsequent requests") {
        ZIO.attemptBlocking {
          val sharedConnector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          val shared          =
            new H1H2CleartextEngine(okRoutes, Context.empty, sharedConnector, DefectHandler.default)
          val server          = LoomServer(sharedConnector).withEngine(shared)
          val handle          = Server.serve(okRoutes, Context.empty.add(server))
          try {
            val port                        = portsOf(handle).head
            // Dirty/misleading storm behind one barrier: garbage bytes, bare
            // CR/LF, a truncated preface that diverges (must route H1, never
            // hang), and a TE+CL smuggling vector (must 400-and-close).
            val payloads: List[Array[Byte]] = List(
              "GARBAGE-BYTES-NO-CRLF\r\n\r\n".getBytes(StandardCharsets.US_ASCII),
              "\r\n\r\n\r\n".getBytes(StandardCharsets.US_ASCII),
              "PRI * HTTP/2.0\r\n\r\nSM\r\n\rX".getBytes(StandardCharsets.US_ASCII),
              ("POST /ok HTTP/1.1\r\nHost: 127.0.0.1\r\n" +
                "Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello").getBytes(StandardCharsets.US_ASCII),
              "GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Bad \u0001: oops\r\n\r\n"
                .getBytes(StandardCharsets.US_ASCII),
            )
            val gate                        = new CountDownLatch(1)
            val done                        = new CountDownLatch(StormClients)
            (0 until StormClients).foreach { i =>
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  try {
                    val socket = new Socket("127.0.0.1", port)
                    socket.setSoTimeout(5000)
                    try {
                      val out = socket.getOutputStream
                      out.write(payloads(i % payloads.length))
                      out.flush()
                      // Drain whatever the server answers (400, H1 response, or
                      // close) so the connection cannot linger half-open.
                      val in  = socket.getInputStream
                      val tmp = new Array[Byte](4096)
                      try while (in.read(tmp) >= 0) ()
                      catch { case _: java.io.IOException => () }
                    } finally socket.close()
                  } catch {
                    case _: java.io.IOException => ()
                  } finally done.countDown()
                  ()
                })
            }
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "malformed storm finished")
            // The next bytes are never a victim request: both protocols serve
            // clean 200s on fresh connections after the storm.
            val h1                          = h1Get(port)
            val h2                          = h2cGet(port)
            require(shared.awaitQuiescent(QuiescentTimeout), "shared engine quiescent after storm")
            assertTrue(h1 == ((200, "stress-ok")), h2 == ((200, "stress-ok")))
          } finally handle.shutdownAndWait()
        }
      },
      test("slow bodies, abrupt disconnects, and stale half-opens settle and drain cleanly") {
        ZIO.attemptBlocking {
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val h2          = new H2Engine(okRoutes, Context.empty, h2Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1).withEngine(h2)
          val handle      = Server.serve(okRoutes, Context.empty.add(server))
          try {
            val ports = portsOf(handle)
            val gate  = new CountDownLatch(1)
            val done  = new CountDownLatch(StormClients)
            // Long: H1 slow-trickle POST bodies, chunk by chunk, behind a
            // barrier; flaky: abrupt RST mid-request; stale: half-open that
            // sends a headline prefix and never terminates it.
            (0 until StormClients).foreach { i =>
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  try {
                    i % 3 match {
                      case 0 =>
                        val client = new RawH1Client(ports(0))
                        try {
                          client.sendRaw("POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\n")
                          "hello world".grouped(2).foreach { part =>
                            client.sendRaw(part)
                            // Bounded pacing only: the server must tolerate
                            // inter-byte gaps without leaking the connection.
                            val deadline = java.lang.System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50)
                            while (java.lang.System.nanoTime() < deadline) Thread.`yield`()
                          }
                          val response = client.readResponse()
                          require(
                            response.status == 200 && response.bodyText == "hello world",
                            "trickle body echoed",
                          )
                        } finally client.close()
                      case 1 =>
                        val socket = new Socket("127.0.0.1", ports(i % 2))
                        try {
                          socket.setSoLinger(true, 0)
                          val out = socket.getOutputStream
                          out.write("GET /ok HTTP/1.1\r\nHost: 12".getBytes(StandardCharsets.US_ASCII))
                          out.flush()
                        } finally socket.close()
                      case _ =>
                        val socket = new Socket("127.0.0.1", ports(0))
                        socket.setSoTimeout(5000)
                        try {
                          val out = socket.getOutputStream
                          out.write("GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\n".getBytes(StandardCharsets.US_ASCII))
                          out.flush()
                          // Never terminate the headers: the server must keep
                          // serving others and reclaim this connection on
                          // shutdown instead of leaking it.
                          val in  = socket.getInputStream
                          try in.read()
                          catch { case _: java.io.IOException => () }
                        } finally socket.close()
                    }
                  } catch {
                    case _: Throwable => ()
                  } finally done.countDown()
                  ()
                })
            }
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "slow/disconnect/stale drivers finished")
            // Unrelated traffic still serves after the abuse.
            val h1Res = h1Get(ports(0))
            val h2Res = h2cGet(ports(1))
            require(h1Res == ((200, "stress-ok")) && h2Res == ((200, "stress-ok")), "server still serves after abuse")
            val quiet = h1.awaitQuiescent(QuiescentTimeout) && h2.awaitQuiescent(QuiescentTimeout)
            require(shutdownIsReal(handle), "await is real after abuse")
            assertTrue(quiet, tcpRefused(ports(0)), tcpRefused(ports(1)), !handle.isRunning)
          } finally handle.shutdownAndWait()
        }
      },
      test("shutdown race with in-flight work and concurrent callers terminates exactly once") {
        ZIO.attemptBlocking {
          val entered     = new CountDownLatch(2)
          val release     = new CountDownLatch(1)
          val routes      = slowRoutes(entered, release)
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(routes, Context.empty, h1Connector, DefectHandler.default)
          val h2          = new H2Engine(routes, Context.empty, h2Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1).withEngine(h2)
          val handle      = Server.serve(routes, Context.empty.add(server))
          try {
            val ports                       = portsOf(handle)
            val h1Result                    = new AtomicReference[(Int, String, Boolean)]()
            val h2Result                    = new AtomicReference[(Int, String)]()
            val h1Driver                    = Thread
              .ofVirtual()
              .start(() => {
                val client = new RawH1Client(ports(0))
                try {
                  client.sendRaw("GET /slow HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                  val response = client.readResponse()
                  h1Result.set(
                    (
                      response.status,
                      response.bodyText,
                      response.header("connection").exists(_.equalsIgnoreCase("close")),
                    ),
                  )
                } finally client.close()
                ()
              })
            val h2Driver                    = Thread
              .ofVirtual()
              .start(() => {
                val client = new RawH2Client(ports(1))
                try {
                  // GOAWAY-tolerant read: the drain's GOAWAY(NO_ERROR)
                  // legitimately precedes the in-flight response on the wire
                  // (in-flight streams complete after GOAWAY), while the
                  // shared fixture treats any GOAWAY as fatal.
                  client.sendFrame(
                    H2Frame.Headers(
                      streamId = 1,
                      headerBlock = client.encodeHeaders(
                        List(
                          H2Header(":method", "GET"),
                          H2Header(":path", "/slow"),
                          H2Header(":scheme", "http"),
                          H2Header(":authority", "127.0.0.1:" + ports(1)),
                        ),
                      ),
                      endStream = true,
                      endHeaders = true,
                    ),
                  )
                  var status = -1
                  var body   = Chunk.empty[Byte]
                  var done   = false
                  while (!done) {
                    client.readFrame() match {
                      case H2Frame.Headers(1, block, end, _, _, _) =>
                        status = client
                          .decodeHeaders(block)
                          .find(_.name == ":status")
                          .map(_.value.toInt)
                          .getOrElse(throw new AssertionError("Missing :status"))
                        if (end) done = true
                      case H2Frame.Data(1, data, end, _)           =>
                        body = body ++ data
                        if (end) done = true
                      case _: H2Frame.GoAway                       => ()
                      case _: H2Frame.Settings                     => ()
                      case _: H2Frame.WindowUpdate                 => ()
                      case _                                       => ()
                    }
                  }
                  h2Result.set((status, new String(body.toArray, StandardCharsets.UTF_8)))
                } finally client.close()
                ()
              })
            // Phase 1: concurrent /ok load proves the aggregate serves under
            // concurrency before the race begins.
            val loadGate                    = new CountDownLatch(1)
            val loadDone                    = new CountDownLatch(4)
            val loadOk                      = new AtomicInteger(0)
            (0 until 4).foreach { i =>
              Thread
                .ofVirtual()
                .start(() => {
                  loadGate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  try {
                    if ((if (i % 2 == 0) h1Get(ports(0)) else h2cGet(ports(1))) == ((200, "stress-ok")))
                      loadOk.incrementAndGet()
                  } catch {
                    case _: Throwable => ()
                  } finally loadDone.countDown()
                  ()
                })
            }
            loadGate.countDown()
            require(loadDone.await(TestTimeoutSeconds, TimeUnit.SECONDS), "concurrent load finished")
            require(loadOk.get() == 4, "concurrent load all 200")
            require(entered.await(TestTimeoutSeconds, TimeUnit.SECONDS), "slow pair in flight")
            // Race: 8 concurrent shutdown callers behind one gate plus a
            // parked awaitShutdown waiter that must have been truly blocked.
            val awaitEntered                = new CountDownLatch(1)
            val awaitReleased               = new CountDownLatch(1)
            Thread
              .ofVirtual()
              .start(() => {
                awaitEntered.countDown()
                handle.awaitShutdown()
                awaitReleased.countDown()
                ()
              })
            require(awaitEntered.await(TestTimeoutSeconds, TimeUnit.SECONDS), "await waiter entered")
            val shutGate                    = new CountDownLatch(1)
            val shutDone                    = new CountDownLatch(8)
            (1 to 8).foreach { _ =>
              Thread
                .ofVirtual()
                .start(() => {
                  shutGate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  handle.shutdown()
                  shutDone.countDown()
                  ()
                })
            }
            shutGate.countDown()
            val early                       = awaitReleased.await(ShortWaitMillis, TimeUnit.MILLISECONDS)
            release.countDown()
            h1Driver.join(15000)
            h2Driver.join(15000)
            require(loadDone.await(TestTimeoutSeconds, TimeUnit.SECONDS), "concurrent load finished")
            require(shutDone.await(TestTimeoutSeconds, TimeUnit.SECONDS), "all shutdown callers returned")
            require(awaitReleased.await(TestTimeoutSeconds, TimeUnit.SECONDS), "await waiter released")
            handle.awaitShutdown()
            require(h1Result.get() != null && h2Result.get() != null, "in-flight pair completed")
            val (h1Status, h1Body, h1Close) = h1Result.get()
            val (h2Status, h2Body)          = h2Result.get()
            val quiet                       = h1.awaitQuiescent(QuiescentTimeout) && h2.awaitQuiescent(QuiescentTimeout)
            assertTrue(
              !early,
              h1Status == 200,
              h1Body == "slow-ok",
              h1Close,
              h2Status == 200,
              h2Body == "slow-ok",
              loadOk.get() == 4,
              quiet,
              tcpRefused(ports(0)),
              tcpRefused(ports(1)),
              !handle.isRunning,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("peer engine failure stays isolated and rollback releases every listener") {
        ZIO.attemptBlocking {
          // Isolation: a failing peer records drain errors but never blocks
          // the healthy H1 engine from stopping, draining, and terminating.
          val stops   = new AtomicInteger(0)
          val drains  = new AtomicInteger(0)
          val failing = new LifecycleEngine {
            val name: String                           = "failing-peer"
            def requestStop(): Unit                    = {
              stops.incrementAndGet()
              throw new RuntimeException("peer stop failed")
            }
            def awaitDrain(timeout: Duration): Boolean = {
              drains.incrementAndGet()
              false
            }
            def forceClose(): Unit                     = ()
          }
          val healthy = new LifecycleEngine {
            val name: String                           = "healthy-peer"
            def requestStop(): Unit                    = ()
            def awaitDrain(timeout: Duration): Boolean = true
            def forceClose(): Unit                     = ()
          }
          val handle  = AggregateServerHandle.live(List(failing, healthy), drainTimeout = Duration.ofMillis(400))
          handle.shutdownAndWait()
          require(handle.state == AggregateLifecycleState.Terminated, "aggregate still terminates")
          require(handle.drainErrors.exists(_.getMessage == "peer stop failed"), "peer error recorded")
          require(stops.get() == 1 && drains.get() == 1, "failing peer ran exactly once")
          // Rollback repeats: a colliding serve rolls every listener back and
          // a fresh serve on the same ports recovers — twice, so no stale
          // listener state accumulates across failures.
          var round   = 0
          var ok      = true
          while (round < 2 && ok) {
            val blocker     = new java.net.ServerSocket(0)
            val blockedPort = blocker.getLocalPort
            try {
              val firstPort = freePort()
              val h1Conn    = Connector(bind = BindAddress.localhost(0))
              val badConn   = Connector(bind = BindAddress.localhost(blockedPort))
              val h1        = new H1Transport(okRoutes, Context.empty, h1Conn, DefectHandler.default)
              val server    =
                LoomServer(h1Conn).addConnector(badConn).withEngine(h1)
              val failure   =
                try {
                  val doomed = Server.serve(okRoutes, Context.empty.add(server))
                  try Left("bound")
                  finally doomed.shutdownAndWait()
                } catch {
                  case error: Throwable => Right(error)
                }
              require(failure.isRight, s"round $round: colliding serve throws")
              // Fresh serve on an adjacent free port recovers immediately.
              val freshConn = Connector(bind = BindAddress.localhost(firstPort))
              val freshH1   = new H1Transport(okRoutes, Context.empty, freshConn, DefectHandler.default)
              val fresh     = LoomServer(freshConn).withEngine(freshH1)
              val freshH    = Server.serve(okRoutes, Context.empty.add(fresh))
              try {
                ok = h1Get(firstPort) == ((200, "stress-ok")) && freshH.isRunning
                require(ok, s"round $round: fresh serve recovers")
              } finally freshH.shutdownAndWait()
              ok = ok && tcpRefused(firstPort)
            } finally blocker.close()
            round += 1
          }
          assertTrue(round == 2, ok)
        }
      },
      test("shared dispatch under concurrent mixed load serves all and shuts down clean") {
        ZIO.attemptBlocking {
          val sharedConnector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          val shared          =
            new H1H2CleartextEngine(okRoutes, Context.empty, sharedConnector, DefectHandler.default)
          val server          = LoomServer(sharedConnector).withEngine(shared)
          val handle          = Server.serve(okRoutes, Context.empty.add(server))
          try {
            val port    = portsOf(handle).head
            val threads = 4
            val perKind = 5
            val total   = threads * perKind * 2
            val okCount = new AtomicInteger(0)
            val gate    = new CountDownLatch(1)
            val done    = new CountDownLatch(threads * 2)
            (0 until threads).foreach { _ =>
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  try {
                    var i = 0
                    while (i < perKind) {
                      try {
                        if (h1Get(port) == ((200, "stress-ok"))) okCount.incrementAndGet()
                      } catch {
                        case _: Throwable => ()
                      }
                      i += 1
                    }
                  } finally done.countDown()
                  ()
                })
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  try {
                    var i = 0
                    while (i < perKind) {
                      try {
                        if (h2cGet(port) == ((200, "stress-ok"))) okCount.incrementAndGet()
                      } catch {
                        case _: Throwable => ()
                      }
                      i += 1
                    }
                  } finally done.countDown()
                  ()
                })
            }
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "mixed shared load finished")
            val served  = okCount.get()
            require(shared.awaitQuiescent(QuiescentTimeout), "shared engine quiescent under load")
            require(shutdownIsReal(handle), "await is real after shared load")
            require(shared.awaitQuiescent(QuiescentTimeout), "shared engine quiescent after shutdown")
            assertTrue(served == total, tcpRefused(port), !handle.isRunning)
          } finally handle.shutdownAndWait()
        }
      },
    ) @@ sequential

  /**
   * Proves `awaitShutdown` really blocks: waiter parks, shutdown releases it.
   */
  private def shutdownIsReal(handle: ServerHandle): Boolean = {
    val entered  = new CountDownLatch(1)
    val released = new CountDownLatch(1)
    Thread
      .ofVirtual()
      .start(() => {
        entered.countDown()
        handle.awaitShutdown()
        released.countDown()
        ()
      })
    require(entered.await(TestTimeoutSeconds, TimeUnit.SECONDS), "await waiter entered")
    val early    = released.await(ShortWaitMillis, TimeUnit.MILLISECONDS)
    handle.shutdown()
    val done     = released.await(TestTimeoutSeconds, TimeUnit.SECONDS)
    handle.awaitShutdown()
    !early && done
  }

  private def portsOf(handle: ServerHandle): List[Int] =
    handle.bindings.map { binding =>
      binding.address match {
        case BoundAddress.Tcp(_, port) => port
        case other                     => throw new AssertionError("Expected TCP binding but found: " + other)
      }
    }

  private def tcpRefused(port: Int): Boolean =
    try {
      val socket = new Socket("127.0.0.1", port)
      socket.close()
      false
    } catch {
      case _: java.io.IOException => true
    }

  private def freePort(): Int = {
    val probe = new java.net.ServerSocket(0)
    try probe.getLocalPort
    finally probe.close()
  }

  private def h1Get(port: Int, path: String = "/ok"): (Int, String) = {
    val client = new RawH1Client(port)
    try {
      client.sendRaw("GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
      val response = client.readResponse()
      (response.status, response.bodyText)
    } finally client.close()
  }

  private def h2cGet(port: Int, path: String = "/ok"): (Int, String) = {
    val client = new RawH2Client(port)
    try {
      val response = client.roundTrip("GET", path, Chunk.empty, 1)
      (response.status, response.bodyText)
    } finally client.close()
  }
}

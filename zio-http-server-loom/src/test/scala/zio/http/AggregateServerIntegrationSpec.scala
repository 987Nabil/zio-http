package zio.http

import java.net.Socket
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental
import scala.jdk.CollectionConverters._

import zio._
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.ResultType._
import zio.http.h1.H1RawClientFixture.RawH1Client
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine
import zio.http.h2.H2Error
import zio.http.h2.H2Frame._
import zio.http.h2.H2RawClientFixture.RawH2Client
import zio.http.h2.hpack.HeaderField

/**
 * Todo 8: aggregate multi-engine startup and graceful shutdown over real
 * engines.
 *
 * Proves the Todo 5 lifecycle contract against live H1/H2 engines and real
 * loopback sockets: one `serve` call starts an H1 and an H2 engine together,
 * one shutdown stops both listeners, drains both engines concurrently under a
 * single deadline, force-closes only the survivors, and truly blocks
 * `awaitShutdown` until the terminal state. Partial startup rolls back
 * already-bound listeners in reverse order without leaking.
 *
 * Every coordination point uses latches/barriers with bounded waits or socket
 * timeouts; no sleep-polling anywhere in these tests.
 */
@experimental
object AggregateServerIntegrationSpec extends ZIOSpecDefault {

  private val TestTimeoutSeconds = 30L
  private val ShortWaitMillis    = 300L

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("ok")))
      },
    ),
  )

  /** Ports of a two-connector serve: H1 first, H2 second. */
  private def portsOf(handle: ServerHandle): (Int, Int) = {
    val ports = handle.bindings.map { binding =>
      binding.address match {
        case BoundAddress.Tcp(_, port) => port
        case other                     => throw new AssertionError("Expected TCP binding but found: " + other)
      }
    }
    (ports(0), ports(1))
  }

  private def tcpRefused(port: Int): Boolean =
    try {
      val socket = new Socket("127.0.0.1", port)
      socket.close()
      false
    } catch {
      case _: java.io.IOException => true
    }

  private def h1Get(port: Int, path: String): (Int, String, Map[String, List[String]]) = {
    val client = new RawH1Client(port)
    try {
      client.sendRaw("GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
      val response = client.readResponse()
      (response.status, response.bodyText, response.headers)
    } finally client.close()
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("AggregateServerIntegrationSpec")(
      test("one serve call starts an H1 and an H2 engine together") {
        ZIO.attemptBlocking {
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          // The registered instance owns its connector: serve must start it,
          // not a copy.
          require(h1.connector == h1Connector, "registered H1 engine owns its connector")
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle      = Server.serve(okRoutes, Context.empty.add(server))
          try {
            require(handle.bindings.length == 2, "both connectors bound")
            val (h1Port, h2Port)      = portsOf(handle)
            val (h1Status, h1Body, _) = h1Get(h1Port, "/ok")
            val h2Client              = new RawH2Client(h2Port)
            try {
              val h2Response = h2Client.roundTrip("GET", "/ok", zio.blocks.chunk.Chunk.empty, 1)
              assertTrue(
                h1Status == 200,
                h1Body == "ok",
                h2Response.status == 200,
                h2Response.bodyText == "ok",
              )
            } finally h2Client.close()
          } finally handle.shutdownAndWait()
        }
      },
      test("graceful shutdown completes in-flight work on both engines") {
        ZIO.attemptBlocking {
          val entered     = new CountDownLatch(2)
          val release     = new CountDownLatch(1)
          val routes      = Routes(
            Route(
              RoutePattern(Method.GET, "/slow-h1"),
              handler { (_: Request) =>
                entered.countDown()
                release.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                responseAsResult(Response(status = Status.Ok, body = Body.fromString("slow-h1")))
              },
            ),
            Route(
              RoutePattern(Method.GET, "/slow-h2"),
              handler { (_: Request) =>
                entered.countDown()
                release.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                responseAsResult(Response(status = Status.Ok, body = Body.fromString("slow-h2")))
              },
            ),
          )
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(routes, Context.empty, h1Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle      = Server.serve(routes, Context.empty.add(server))
          try {
            val (h1Port, h2Port)            = portsOf(handle)
            val h1Result                    = new AtomicReference[(Int, String, Boolean)]()
            val h2Result                    = new AtomicReference[(Int, String)]()
            // The drain's GOAWAY is observed on the wire before the release:
            // observing it proves H2 (and, by request-stop order, H1) drained,
            // so the H1 close-header below is deterministic, not racy.
            val goAwaySeen                  = new CountDownLatch(1)
            val h1Driver                    = Thread
              .ofVirtual()
              .start(() => {
                val client = new RawH1Client(h1Port)
                try {
                  client.sendRaw("GET /slow-h1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                  val response = client.readResponse()
                  // A completed 200 proves the drain let in-flight work finish
                  // instead of force-closing it: a force close kills the
                  // connection before any response exists. The authoritative
                  // close marks the drain path (see goAwaySeen ordering above).
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
                val client = new RawH2Client(h2Port)
                try {
                  client.sendFrame(
                    Headers(
                      streamId = 1,
                      headerBlock = client.encodeHeaders(
                        List(
                          HeaderField(":method", "GET"),
                          HeaderField(":path", "/slow-h2"),
                          HeaderField(":scheme", "http"),
                          HeaderField(":authority", "127.0.0.1:" + h2Port),
                        ),
                      ),
                      endStream = true,
                      endHeaders = true,
                    ),
                  )
                  // GOAWAY-tolerant response read: the drain's GOAWAY(NO_ERROR)
                  // legitimately precedes the in-flight response on the wire
                  // (in-flight streams complete after GOAWAY), while the shared
                  // fixture treats any GOAWAY as fatal. Skip connection-level
                  // frames and keep awaiting stream 1.
                  var status = -1
                  var body   = zio.blocks.chunk.Chunk.empty[Byte]
                  var done   = false
                  while (!done) {
                    client.readFrame() match {
                      case Headers(1, block, end, _, _, _) =>
                        val fields = client.decodeHeaders(block)
                        status = fields
                          .find(_.name == ":status")
                          .map(_.value.toInt)
                          .getOrElse(throw new AssertionError("Missing :status"))
                        if (end) done = true
                      case Data(1, data, end, _)           =>
                        body = body ++ data
                        if (end) done = true
                      case GoAway(_, _, _)                 => goAwaySeen.countDown()
                      case _: Settings                     => ()
                      case _: WindowUpdate                 => ()
                      case _                               => ()
                    }
                  }
                  h2Result.set((status, new String(body.toArray, java.nio.charset.StandardCharsets.UTF_8)))
                } finally client.close()
                ()
              })
            require(entered.await(TestTimeoutSeconds, TimeUnit.SECONDS), "both handlers in flight")
            // Both handlers are now in flight; shut down in the background and
            // let the drain observe their completion.
            val shutdownDone                = new CountDownLatch(1)
            Thread
              .ofVirtual()
              .start(() => {
                handle.shutdown()
                shutdownDone.countDown()
                ()
              })
            require(goAwaySeen.await(TestTimeoutSeconds, TimeUnit.SECONDS), "drain reached the H2 wire")
            release.countDown()
            h1Driver.join(15000)
            h2Driver.join(15000)
            require(shutdownDone.await(TestTimeoutSeconds, TimeUnit.SECONDS), "shutdown returned")
            handle.awaitShutdown()
            require(h1Result.get() != null, "H1 in-flight response arrived")
            require(h2Result.get() != null, "H2 in-flight response arrived")
            val (h1Status, h1Body, h1Close) = h1Result.get()
            val (h2Status, h2Body)          = h2Result.get()
            assertTrue(
              h1Status == 200,
              h1Body == "slow-h1",
              h1Close,
              h2Status == 200,
              h2Body == "slow-h2",
              !handle.isRunning,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("awaitShutdown blocks until the aggregate reaches its terminal state") {
        ZIO.attemptBlocking {
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle      = Server.serve(okRoutes, Context.empty.add(server))
          try {
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
            require(entered.await(TestTimeoutSeconds, TimeUnit.SECONDS), "waiter entered")
            val early    = released.await(ShortWaitMillis, TimeUnit.MILLISECONDS)
            handle.shutdown()
            require(released.await(TestTimeoutSeconds, TimeUnit.SECONDS), "waiter released")
            val prompt   = handle.awaitShutdown(Duration.ofSeconds(15))
            handle.awaitShutdown()
            assertTrue(!early, prompt, !handle.isRunning)
          } finally handle.shutdownAndWait()
        }
      },
      test("shutdown stops accepting on both listeners") {
        ZIO.attemptBlocking {
          val h1Connector      = Connector(bind = BindAddress.localhost(0))
          val h2Connector      = Connector(bind = BindAddress.localhost(0))
          val h1               = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val server           = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle           = Server.serve(okRoutes, Context.empty.add(server))
          val (h1Port, h2Port) = portsOf(handle)
          // Both ports serve before shutdown.
          val (before, _, _)   = h1Get(h1Port, "/ok")
          require(before == 200, "H1 serves before shutdown")
          handle.shutdownAndWait()
          // Both listeners refuse new work after shutdown.
          val h2Refused        =
            try {
              val client = new RawH2Client(h2Port)
              client.close()
              false
            } catch {
              case _: java.io.IOException => true
            }
          assertTrue(tcpRefused(h1Port), h2Refused, !handle.isRunning)
        }
      },
      test("repeated shutdown calls are safe") {
        ZIO.attemptBlocking {
          val h1Connector      = Connector(bind = BindAddress.localhost(0))
          val h2Connector      = Connector(bind = BindAddress.localhost(0))
          val h1               = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val server           = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle           = Server.serve(okRoutes, Context.empty.add(server))
          val (h1Port, h2Port) = portsOf(handle)
          handle.shutdown()
          handle.shutdownAndWait()
          handle.close()
          assertTrue(tcpRefused(h1Port), tcpRefused(h2Port), !handle.isRunning)
        }
      },
      test("concurrent shutdown callers terminate the aggregate exactly once") {
        ZIO.attemptBlocking {
          val h1Connector      = Connector(bind = BindAddress.localhost(0))
          val h2Connector      = Connector(bind = BindAddress.localhost(0))
          val h1               = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val server           = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1)
          val handle           = Server.serve(okRoutes, Context.empty.add(server))
          val (h1Port, h2Port) = portsOf(handle)
          val gate             = new CountDownLatch(1)
          val done             = new CountDownLatch(8)
          (1 to 8).foreach { _ =>
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                handle.shutdown()
                done.countDown()
                ()
              })
          }
          gate.countDown()
          require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "all callers returned")
          handle.awaitShutdown()
          assertTrue(tcpRefused(h1Port), tcpRefused(h2Port), !handle.isRunning)
        }
      },
      test("partial bind failure rolls back the bound listener and spares the survivor") {
        ZIO.attemptBlocking {
          // Fixed target port held by a survivor server for the whole test.
          val survivorConnector = Connector(bind = BindAddress.localhost(0))
          val survivor          =
            ServerHandle.live(
              List(new H1Transport(okRoutes, Context.empty, survivorConnector, DefectHandler.default).start()),
            )
          try {
            val survivorPort                      = survivor.bindings.head.address match {
              case BoundAddress.Tcp(_, port) => port
              case other                     => throw new AssertionError("Expected TCP binding but found: " + other)
            }
            // A free fixed port for the doomed first connector.
            val probe                             = new java.net.ServerSocket(0)
            val freePort                          = probe.getLocalPort
            probe.close()
            val good                              = Connector(bind = BindAddress.localhost(freePort))
            val bad                               = Connector(bind = BindAddress.localhost(survivorPort))
            val server                            = LoomServer(good).addConnector(bad)
            val failure                           =
              try {
                val handle = Server.serve(okRoutes, Context.empty.add(server))
                try Left("bound")
                finally handle.shutdownAndWait()
              } catch {
                case error: Throwable => Right(error)
              }
            require(failure.isRight, "conflicting serve throws")
            // The rolled-back listener released its port: nothing accepts there.
            val rolledBack                        = tcpRefused(freePort)
            // The survivor never noticed: still serving.
            val (survivorStatus, survivorBody, _) = h1Get(survivorPort, "/ok")
            assertTrue(rolledBack, survivorStatus == 200, survivorBody == "ok")
          } finally survivor.shutdownAndWait()
        }
      },
      test("bindEngines rolls back bound engines in reverse order with suppressed errors") {
        ZIO.attemptBlocking {
          val log = new java.util.concurrent.ConcurrentLinkedQueue[String]()
          def fakeEngine(name: String, failOnClose: Boolean): ProtocolEngine = new ProtocolEngine {
            val id: EngineId                        = EngineId(name)
            val transportKind: TransportKind        = TransportKind.Tcp
            val supportedProtocols: Set[ProtocolId] = Set[ProtocolId](ProtocolId.Http1)
            def drain(): Unit                       = ()
            def close(): Unit                       = {
              log.add("close-" + name)
              if (failOnClose) throw new RuntimeException("rollback failed: " + name)
            }
          }
          def fakeBound(name: String): BoundConnectorHandle                  =
            BoundConnectorHandle(
              BoundConnector(BoundAddress.Tcp("127.0.0.1", 19000 + log.size()), Protocol.H2C()),
              () => {
                log.add("unbind-" + name)
                ()
              },
              () => true,
            )
          val binders: List[() => BoundProtocolEngine]                       = List(
            () => new BoundProtocolEngine(fakeEngine("e1", failOnClose = true), fakeBound("e1")),
            () => new BoundProtocolEngine(fakeEngine("e2", failOnClose = false), fakeBound("e2")),
            () => throw new RuntimeException("bind e3 failed"),
          )
          val failure                                                        =
            try {
              AggregateServerHandle.bindEngines(binders)
              Left("bound")
            } catch {
              case error: Throwable => Right(error)
            }
          assertTrue(
            failure.isRight,
            failure.map(_.getMessage) == Right("bind e3 failed"),
            failure.map(_.getSuppressed.length) == Right(1),
            log.asScala.toList == List("close-e2", "unbind-e2", "close-e1", "unbind-e1"),
          )
        }
      },
      test("drain deadline force-closes a parked H2 waiter and a blocked H1 reader") {
        ZIO.attemptBlocking {
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val h2          = new H2Engine(okRoutes, Context.empty, h2Connector, DefectHandler.default)
          val handle      = AggregateServerHandle.bindEngines(
            List(() => new BoundProtocolEngine(h1, h1.start()), () => new BoundProtocolEngine(h2, h2.start())),
            drainTimeout = Duration.ofMillis(400),
          )
          try {
            val (h1Port, h2Port) = portsOf(handle)
            // Park an H2 stream mid-body: headers sent open, body never follows.
            val h2Client         = new RawH2Client(h2Port)
            h2Client.sendFrame(
              Headers(
                streamId = 1,
                headerBlock = h2Client.encodeHeaders(
                  List(
                    HeaderField(":method", "POST"),
                    HeaderField(":path", "/ok"),
                    HeaderField(":scheme", "http"),
                    HeaderField(":authority", "127.0.0.1:" + h2Port),
                  ),
                ),
                endStream = false,
                endHeaders = true,
              ),
            )
            // Park an H1 connection mid-headline: headers never terminate.
            val h1Client         = new RawH1Client(h1Port)
            h1Client.sendRaw("GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\n")
            // Neither waiter can complete on its own: shutdown must enforce
            // the single deadline and force-close both survivors.
            val shutdownDone     = new CountDownLatch(1)
            Thread
              .ofVirtual()
              .start(() => {
                handle.shutdown()
                shutdownDone.countDown()
                ()
              })
            require(shutdownDone.await(TestTimeoutSeconds, TimeUnit.SECONDS), "shutdown met its deadline")
            handle.awaitShutdown()
            // The drain's GOAWAY arrives first (buffered), then the force
            // close tears the connection down: the read ends only at EOF.
            var sawGoAway        = false
            var h2Dead           = false
            var h2Done           = false
            while (!h2Done) {
              try
                h2Client.readFrame() match {
                  case GoAway(_, code, _) => sawGoAway = code == H2Error.Code.NO_ERROR
                  case _                  => ()
                }
              catch {
                case _: java.io.IOException =>
                  h2Dead = true
                  h2Done = true
              }
            }
            h2Client.close()
            // The blocked H1 reader never saw a response: the force close ends
            // it at EOF.
            var h1Dead           = false
            var h1Done           = false
            while (!h1Done) {
              try {
                if (h1Client.readByte() < 0) {
                  h1Dead = true
                  h1Done = true
                }
              } catch {
                case _: java.io.IOException =>
                  h1Dead = true
                  h1Done = true
              }
            }
            h1Client.close()
            // In-flight accounting settled on both engines: no waiter, timer,
            // or connection residue survived the force close. (A connection
            // left alive would keep its tracker non-empty and fail these
            // bounds instead of hanging the suite.)
            val h1Quiet          = h1.awaitQuiescent(Duration.ofSeconds(15))
            val h2Quiet          = h2.awaitQuiescent(Duration.ofSeconds(15))
            assertTrue(sawGoAway, h2Dead, h1Dead, h1Quiet, h2Quiet, !handle.isRunning)
          } finally handle.shutdownAndWait()
        }
      },
      test("H1 idle timer fires and the aggregate still shuts down cleanly") {
        ZIO.attemptBlocking {
          val idleConnector = Connector(bind = BindAddress.localhost(0), idleTimeout = Duration.ofMillis(500))
          val h1            = new H1Transport(okRoutes, Context.empty, idleConnector, DefectHandler.default)
          val server        = LoomServer(idleConnector).withEngine(h1)
          val handle        = Server.serve(okRoutes, Context.empty.add(server))
          try {
            val port              = handle.bindings.head.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP binding but found: " + other)
            }
            val client            = new RawH1Client(port)
            try {
              // No bytes sent: the idle timer must close the connection.
              val eof = client.readByte()
              require(eof == -1, "idle connection closed by the server")
            } finally client.close()
            // The server is healthy after the timer fired: it still serves.
            val (status, body, _) = h1Get(port, "/ok")
            require(status == 200 && body == "ok", "server still serves after timer expiry")
          } finally handle.shutdownAndWait()
          assertTrue(!handle.isRunning)
        }
      },
    ) @@ sequential
}

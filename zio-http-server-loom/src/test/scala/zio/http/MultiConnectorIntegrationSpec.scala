package zio.http

import java.net.Socket
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
import zio.http.h2.H2Frame._
import zio.http.h2.H2RawClientFixture.RawH2Client
import zio.http.h2.hpack.HeaderField

/**
 * Todo 13 RED: concurrent protocol sets and connectors under one server.
 *
 * Live wire proofs that one `LoomServer.serve` call runs several connectors —
 * H1-only, H2-only, shared H1/H2 (cleartext preface and TLS ALPN) — on one
 * routes definition, one context, and one aggregate lifecycle:
 *   - H1-only plus H2-only connectors serve concurrently with accurate bindings
 *     (distinct ephemeral ports, per-connector protocols);
 *   - a shared cleartext port plus a strict-TLS H2 port serve concurrently;
 *   - a TLS-shared port plus an H2C-only port serve concurrently;
 *   - duplicate protocols across connectors fail fast before any socket binds;
 *   - a bind collision rolls back every connector (all-or-nothing) and a fresh
 *     serve on the same ports succeeds afterwards (no stale state);
 *   - one shutdown drains in-flight work on every connector and `awaitShutdown`
 *     truly blocks until the terminal state;
 *   - concurrent clients across three connector types share one routes
 *     definition and one handle.
 *
 * Every wait is a bounded latch wait or a socket timeout; no sleep-polling
 * anywhere in these tests.
 */
@experimental
object MultiConnectorIntegrationSpec extends ZIOSpecDefault {

  private val TestTimeoutSeconds = 30L
  private val ShortWaitMillis    = 300L
  private val BodyText           = "multi-ok"

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      Handler.succeed(Response.text(BodyText)),
    ),
  )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("MultiConnectorIntegrationSpec")(
      test("H1-only and H2-only connectors serve concurrently with accurate bindings") {
        ZIO.attemptBlocking {
          val h1Connector = Connector(bind = BindAddress.localhost(0))
          val h2Connector = Connector(bind = BindAddress.localhost(0))
          val h1          = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val h2          = new H2Engine(okRoutes, Context.empty, h2Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1).withEngine(h2)
          val handle      = Server.serve(okRoutes, Context.empty.add(server))
          try {
            require(handle.bindings.length == 2, "both connectors bound")
            val ports    = portsOf(handle)
            require(ports.distinct.length == 2 && ports.forall(_ != 0), "distinct ephemeral ports")
            require(
              protocolsOf(handle) == List(h1Connector.protocol, h2Connector.protocol),
              "bindings carry per-connector protocols",
            )
            val h1Result = new AtomicReference[(Int, String)]()
            val h2Result = new AtomicReference[(Int, String)]()
            val gate     = new CountDownLatch(1)
            val done     = new CountDownLatch(2)
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                h1Result.set(h1Get(ports(0)))
                done.countDown()
                ()
              })
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                h2Result.set(h2cGet(ports(1)))
                done.countDown()
                ()
              })
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "both connector types answered")
            require(h1Result.get() != null, "H1 response arrived")
            require(h2Result.get() != null, "H2 response arrived")
            // Smart-assert bodies observe post-finally state (see TestArrow laziness):
            // extract every live read into a stable val before asserting.
            val running  = handle.isRunning
            assertTrue(
              h1Result.get() == ((200, BodyText)),
              h2Result.get() == ((200, BodyText)),
              running,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("shared cleartext and strict-TLS H2 connectors serve concurrently") {
        ZIO.attemptBlocking {
          val sharedConnector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          val tlsConnector    = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig(List("h2"), AlpnPolicy.StrictH2)),
          )
          val shared          =
            new H1H2CleartextEngine(okRoutes, Context.empty, sharedConnector, DefectHandler.default)
          val h2tls           = new H2Engine(okRoutes, Context.empty, tlsConnector, DefectHandler.default)
          val server          =
            LoomServer(sharedConnector).addConnector(tlsConnector).withEngine(shared).withEngine(h2tls)
          val handle          = Server.serve(okRoutes, Context.empty.add(server))
          try {
            require(handle.bindings.length == 2, "both connectors bound")
            val ports    = portsOf(handle)
            require(ports.distinct.length == 2 && ports.forall(_ != 0), "distinct ephemeral ports")
            require(
              protocolsOf(handle) == List(sharedConnector.protocol, tlsConnector.protocol),
              "bindings carry per-connector protocols",
            )
            val sharedH1 = new AtomicReference[(Int, String)]()
            val sharedH2 = new AtomicReference[(Int, String)]()
            val tlsH2    = new AtomicReference[(Int, String)]()
            val gate     = new CountDownLatch(1)
            val done     = new CountDownLatch(3)
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                sharedH1.set(h1Get(ports(0)))
                done.countDown()
                ()
              })
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                sharedH2.set(h2cGet(ports(0)))
                done.countDown()
                ()
              })
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                val client         = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
                val (_, status, _) = TlsAlpnFixtures.jdkGet(client, ports(1), "/ok")
                tlsH2.set((status, BodyText))
                done.countDown()
                ()
              })
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "shared and TLS ports answered")
            require(sharedH1.get() != null && sharedH2.get() != null && tlsH2.get() != null, "all got 200")
            val running  = handle.isRunning
            assertTrue(
              sharedH1.get() == ((200, BodyText)),
              sharedH2.get() == ((200, BodyText)),
              tlsH2.get() == ((200, BodyText)),
              running,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("TLS-shared and H2C-only connectors serve concurrently") {
        ZIO.attemptBlocking {
          val tlsConnector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig()),
          )
          val h2cConnector = Connector(bind = BindAddress.localhost(0))
          val tlsShared    = new H1H2TlsEngine(okRoutes, Context.empty, tlsConnector, DefectHandler.default)
          val h2cOnly      = new H2Engine(okRoutes, Context.empty, h2cConnector, DefectHandler.default)
          val server       =
            LoomServer(tlsConnector).addConnector(h2cConnector).withEngine(tlsShared).withEngine(h2cOnly)
          val handle       = Server.serve(okRoutes, Context.empty.add(server))
          try {
            require(handle.bindings.length == 2, "both connectors bound")
            val ports   = portsOf(handle)
            require(ports.distinct.length == 2 && ports.forall(_ != 0), "distinct ephemeral ports")
            require(
              protocolsOf(handle) == List(tlsConnector.protocol, h2cConnector.protocol),
              "bindings carry per-connector protocols",
            )
            val tlsH1   = new AtomicReference[(String, Int, String)]()
            val tlsH2   = new AtomicReference[(Int, String)]()
            val h2c     = new AtomicReference[(Int, String)]()
            val gate    = new CountDownLatch(1)
            val done    = new CountDownLatch(3)
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                tlsH1.set(TlsAlpnFixtures.rawTlsH1Get(ports(0), Array("http/1.1"), "/ok"))
                done.countDown()
                ()
              })
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                val client                  = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
                val (version, status, body) = TlsAlpnFixtures.jdkGet(client, ports(0), "/ok")
                require(version == java.net.http.HttpClient.Version.HTTP_2, "TLS H2 negotiated h2")
                tlsH2.set((status, body))
                done.countDown()
                ()
              })
            Thread
              .ofVirtual()
              .start(() => {
                gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                h2c.set(h2cGet(ports(1)))
                done.countDown()
                ()
              })
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "TLS-shared and H2C ports answered")
            require(tlsH1.get() != null && tlsH2.get() != null && h2c.get() != null, "all answered")
            val running = handle.isRunning
            assertTrue(
              tlsH1.get() == (("http/1.1", 200, BodyText)),
              tlsH2.get() == ((200, BodyText)),
              h2c.get() == ((200, BodyText)),
              running,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("duplicate protocols across connectors fail fast before any socket binds") {
        ZIO.attemptBlocking {
          val firstPort     = freePort()
          val secondPort    = freePort()
          val h1Conn        = Connector(bind = BindAddress.localhost(firstPort))
          val sharedConn    = Connector(
            bind = BindAddress.localhost(secondPort),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          val h1            = new H1Transport(okRoutes, Context.empty, h1Conn, DefectHandler.default)
          val shared        = new H1H2CleartextEngine(okRoutes, Context.empty, sharedConn, DefectHandler.default)
          val server        = LoomServer(h1Conn).addConnector(sharedConn).withEngine(h1).withEngine(shared)
          val failure       =
            try {
              val handle = Server.serve(okRoutes, Context.empty.add(server))
              try Left("bound")
              finally handle.shutdownAndWait()
            } catch {
              case error: Throwable => Right(error)
            }
          val duplicate     = failure match {
            case Right(_: EngineRegistrationError.DuplicateProtocol) => true
            case _                                                   => false
          }
          require(duplicate, "duplicate Http1 fails fast with DuplicateProtocol")
          val firstRefused  = tcpRefused(firstPort)
          val secondRefused = tcpRefused(secondPort)
          assertTrue(firstRefused, secondRefused)
        }
      },
      test("bind collision rolls back every connector and a fresh serve recovers") {
        ZIO.attemptBlocking {
          val blocker     = new java.net.ServerSocket(0)
          val blockedPort = blocker.getLocalPort
          try {
            val firstPort  = freePort()
            val secondPort = freePort()
            val h1Conn     = Connector(bind = BindAddress.localhost(firstPort))
            val h2Conn     = Connector(bind = BindAddress.localhost(secondPort))
            val badConn    = Connector(bind = BindAddress.localhost(blockedPort))
            val h1         = new H1Transport(okRoutes, Context.empty, h1Conn, DefectHandler.default)
            val h2         = new H2Engine(okRoutes, Context.empty, h2Conn, DefectHandler.default)
            // addConnector prepends: add the doomed connector first so the bind
            // order is h1, h2, bad and the rollback covers both live listeners.
            val server     = LoomServer(h1Conn).addConnector(badConn).addConnector(h2Conn).withEngine(h1).withEngine(h2)
            val failure    =
              try {
                val handle = Server.serve(okRoutes, Context.empty.add(server))
                try Left("bound")
                finally handle.shutdownAndWait()
              } catch {
                case error: Throwable => Right(error)
              }
            require(failure.isRight, "colliding serve throws")
            require(tcpRefused(firstPort), "first connector rolled back")
            require(tcpRefused(secondPort), "second connector rolled back")
            // No stale state: the same ports serve fresh immediately afterwards.
            val freshH1Conn = Connector(bind = BindAddress.localhost(firstPort))
            val freshH2Conn = Connector(bind = BindAddress.localhost(secondPort))
            val freshH1     = new H1Transport(okRoutes, Context.empty, freshH1Conn, DefectHandler.default)
            val freshH2     = new H2Engine(okRoutes, Context.empty, freshH2Conn, DefectHandler.default)
            val fresh       =
              LoomServer(freshH1Conn).addConnector(freshH2Conn).withEngine(freshH1).withEngine(freshH2)
            val freshHandle = Server.serve(okRoutes, Context.empty.add(fresh))
            try {
              val freshPorts = portsOf(freshHandle)
              require(freshPorts == List(firstPort, secondPort), "rebound addresses are accurate")
              // Eager live reads: smart-assert bodies observe post-finally state.
              val freshH1    = h1Get(firstPort)
              val freshH2    = h2cGet(secondPort)
              assertTrue(freshH1 == ((200, BodyText)), freshH2 == ((200, BodyText)))
            } finally freshHandle.shutdownAndWait()
          } finally blocker.close()
        }
      },
      test("one shutdown drains in-flight work on every connector") {
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
          val h2          = new H2Engine(routes, Context.empty, h2Connector, DefectHandler.default)
          val server      = LoomServer(h1Connector).addConnector(h2Connector).withEngine(h1).withEngine(h2)
          val handle      = Server.serve(routes, Context.empty.add(server))
          try {
            val ports                       = portsOf(handle)
            val h1Result                    = new AtomicReference[(Int, String, Boolean)]()
            val h2Result                    = new AtomicReference[(Int, String)]()
            val goAwaySeen                  = new CountDownLatch(1)
            val h1Driver                    = Thread
              .ofVirtual()
              .start(() => {
                val client = new RawH1Client(ports(0))
                try {
                  client.sendRaw("GET /slow-h1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
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
                  client.sendFrame(
                    Headers(
                      streamId = 1,
                      headerBlock = client.encodeHeaders(
                        List(
                          HeaderField(":method", "GET"),
                          HeaderField(":path", "/slow-h2"),
                          HeaderField(":scheme", "http"),
                          HeaderField(":authority", "127.0.0.1:" + ports(1)),
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
            val shutdownDone                = new CountDownLatch(1)
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
            Thread
              .ofVirtual()
              .start(() => {
                handle.shutdown()
                shutdownDone.countDown()
                ()
              })
            val early                       = awaitReleased.await(ShortWaitMillis, TimeUnit.MILLISECONDS)
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
            require(awaitReleased.await(TestTimeoutSeconds, TimeUnit.SECONDS), "await waiter released")
            val h1Refused                   = tcpRefused(ports(0))
            val h2Refused                   = tcpRefused(ports(1))
            val terminated                  = !handle.isRunning
            assertTrue(
              !early,
              h1Status == 200,
              h1Body == "slow-h1",
              h1Close,
              h2Status == 200,
              h2Body == "slow-h2",
              h1Refused,
              h2Refused,
              terminated,
            )
          } finally handle.shutdownAndWait()
        }
      },
      test("concurrent clients across three connector types share one routes definition") {
        ZIO.attemptBlocking {
          val h1Connector  = Connector(bind = BindAddress.localhost(0))
          val h2cConnector = Connector(bind = BindAddress.localhost(0))
          val tlsConnector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig(List("h2"), AlpnPolicy.StrictH2)),
          )
          val h1           = new H1Transport(okRoutes, Context.empty, h1Connector, DefectHandler.default)
          val h2c          = new H2Engine(okRoutes, Context.empty, h2cConnector, DefectHandler.default, EngineId("h2c"))
          val h2tls        =
            new H2Engine(okRoutes, Context.empty, tlsConnector, DefectHandler.default, EngineId("h2-tls"))
          // addConnector prepends: add tls before h2c so the serve order is
          // h1, h2c, tls and ports(1)/ports(2) match the assertions below.
          val server       = LoomServer(h1Connector)
            .addConnector(tlsConnector)
            .addConnector(h2cConnector)
            .withEngine(h1)
            .withEngine(h2c)
            .withEngine(h2tls)
          val handle       = Server.serve(okRoutes, Context.empty.add(server))
          try {
            require(handle.bindings.length == 3, "all three connectors bound")
            val ports      = portsOf(handle)
            require(ports.distinct.length == 3 && ports.forall(_ != 0), "distinct ephemeral ports")
            require(
              protocolsOf(handle) == List(h1Connector.protocol, h2cConnector.protocol, tlsConnector.protocol),
              "bindings carry per-connector protocols",
            )
            val jdkClient  = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
            val okCount    = new AtomicInteger(0)
            val threads    = 2
            val iterations = 5
            val total      = threads * iterations * 3
            val gate       = new CountDownLatch(1)
            val done       = new CountDownLatch(threads * 3)
            (1 to threads).foreach { _ =>
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  var i = 0
                  while (i < iterations) {
                    if (h1Get(ports(0)) == ((200, BodyText))) okCount.incrementAndGet()
                    i += 1
                  }
                  done.countDown()
                  ()
                })
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  var i = 0
                  while (i < iterations) {
                    if (h2cGet(ports(1)) == ((200, BodyText))) okCount.incrementAndGet()
                    i += 1
                  }
                  done.countDown()
                  ()
                })
              Thread
                .ofVirtual()
                .start(() => {
                  gate.await(TestTimeoutSeconds, TimeUnit.SECONDS)
                  var i = 0
                  while (i < iterations) {
                    val (_, status, body) = TlsAlpnFixtures.jdkGet(jdkClient, ports(2), "/ok")
                    if (status == 200 && body == BodyText) okCount.incrementAndGet()
                    i += 1
                  }
                  done.countDown()
                  ()
                })
            }
            gate.countDown()
            require(done.await(TestTimeoutSeconds, TimeUnit.SECONDS), "every client finished")
            val running    = handle.isRunning
            assertTrue(okCount.get() == total, running)
          } finally handle.shutdownAndWait()
        }
      },
    ) @@ sequential

  /** Bound TCP ports in binding order. */
  private def portsOf(handle: ServerHandle): List[Int] =
    handle.bindings.map { binding =>
      binding.address match {
        case BoundAddress.Tcp(_, port) => port
        case other                     => throw new AssertionError("Expected TCP binding but found: " + other)
      }
    }

  /** Bound protocols in binding order. */
  private def protocolsOf(handle: ServerHandle): List[Protocol] =
    handle.bindings.map(_.protocol)

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

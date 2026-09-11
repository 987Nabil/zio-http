package zio.http.conformance

import java.io.EOFException
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.annotation.experimental

import zio.Scope
import zio.ZIO
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.blocks.streams.Stream
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.ResultType._
import zio.http.h1.H1Transport
import zio.http.h2.FrameCodec
import zio.http.h2.H2Error
import zio.http.h2.H2Engine
import zio.http.h2.H2Frame
import zio.http.h2.H2Frame._
import zio.http.h2.hpack.HeaderField
import zio.http.h2.hpack.HpackDecoder
import zio.http.h2.hpack.HpackEncoder
import zio.http.sse.ServerSentEvent
import zio.http.sse.Sse
import zio.http.sse.SseCodec
import zio.http.TlsAlpnFixtures
import zio.http.{
  AccessLogRecord,
  AccessLogSink,
  BindAddress,
  Body,
  BoundAddress,
  Connector,
  DefectHandler,
  H1H2CleartextEngine,
  H1H2TlsEngine,
  Handler,
  Header,
  LoomServer,
  Method,
  Middleware,
  NegotiationPolicy,
  Protocol,
  Request,
  Response,
  Route,
  Routes,
  Server,
  Status,
  TrustedProxyConfig,
  handler,
}

/**
 * Todo 16: protocol parity and merged-feature compatibility.
 *
 * One unchanged route corpus ([[ConformanceCorpus.routes]]) served through H1,
 * H2C, TLS H2, shared TLS and shared cleartext via [[ProtocolParityBackends]]:
 * 7 legs × the 14-test [[ConformanceHarness.suiteFor]] corpus. The
 * `mergedFeatures` suite then pins the merged #4291
 * (limits/deadlines/proxy/access log), #4292 (streaming/SSE/independent
 * clients/endpoint) and #4293 (cookie/session) behaviors across the same
 * transports.
 *
 * Intentional wire differences (declared here, never hidden):
 *   - over-cap bodies: H1 answers 413 and closes; H2C resets the stream
 *     (RST_STREAM) while the connection survives — both reject, never 200;
 *   - expired deadlines: H1 closes the connection; H2C resets the stream — both
 *     deliver no response, never a late 200;
 *   - framing: H1 uses Content-Length/chunked, H2 uses DATA frames — true
 *     clients on each side observe identical application bytes;
 *   - `Upgrade: h2c` on H1 is an ordinary header: plain H1 200, never 101;
 *   - proxy headers: H1 passes forwarding headers through untouched
 *     (transparent transport); H2 consumes them per TrustedProxyConfig
 *     (untrusted peers: stripped with zero effect; trusted loopback peers:
 *     client IP/host/scheme resolved and surfaced);
 *   - TLS H1 clients must offer `http/1.1` ALPN exactly (a JDK client pinned to
 *     HTTP/1.1 sends none and is closed by policy before any parser).
 */
@experimental
object ProtocolParitySpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ProtocolParitySpec")(
      suite("corpus")(
        ConformanceHarness.suiteFor(ProtocolParityBackends.h1),
        ConformanceHarness.suiteFor(ProtocolParityBackends.h2c),
        ConformanceHarness.suiteFor(ProtocolParityBackends.tlsH2),
        ConformanceHarness.suiteFor(ProtocolParityBackends.sharedTlsH1),
        ConformanceHarness.suiteFor(ProtocolParityBackends.sharedTlsH2),
        ConformanceHarness.suiteFor(ProtocolParityBackends.sharedCleartextH1),
        ConformanceHarness.suiteFor(ProtocolParityBackends.sharedCleartextH2c),
      ),
      suite("mergedFeatures")(
        test("limits: over-cap bodies reject on H1 (413+close) and H2C (stream reset)") {
          val capRoutes: Routes[Any] = Routes(
            Route(
              RoutePattern(Method.POST, "/echo-limited"),
              handler { (req: Request) =>
                responseAsResult(Response(status = Status.Ok, body = Body.fromChunk(req.body.toChunk)))
              },
            ),
          )
          val tiny                   = Connector(bind = BindAddress.localhost(0), maxRequestBodySize = 8L)
          val h1Outcome              = withH1(capRoutes, tiny) { port =>
            ZIO.attemptBlocking {
              val socket = new Socket("127.0.0.1", port)
              socket.setSoTimeout(10000)
              try {
                val out  = socket.getOutputStream
                out.write(
                  ("POST /echo-limited HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 100\r\n\r\n" + ("0123456789" * 10))
                    .getBytes(StandardCharsets.US_ASCII),
                )
                out.flush()
                val wire = new H1WireClient(socket.getInputStream, out)
                val resp = wire.readResponse()
                val eof  =
                  try { socket.getInputStream.read() }
                  catch { case _: java.net.SocketException => -1 }
                (resp.status, eof)
              } finally socket.close()
            }
          }.map { case (status, eof) => assertTrue(status == 413, eof == -1) }
          val h2Outcome              = withH2c(capRoutes, tiny) { port =>
            ZIO.attemptBlocking(h2PostForReset(port, "/echo-limited", ConformanceCorpus.deterministicBytes(1024)))
          }.map(outcome => assertTrue(outcome.startsWith("reset:")))
          val h1Healthy              = withH1(capRoutes, tiny) { port =>
            ZIO.attemptBlocking(new ParityCleartextH1Client(port).request("GET", "/nope", Nil, Chunk.empty).status)
          }.map(status => assertTrue(status == 404))
          for {
            h1     <- h1Outcome
            h2     <- h2Outcome
            health <- h1Healthy
          } yield h1 && h2 && health
        },
        test("deadlines: stalled H1 closes, stalled H2C stream resets, neither serves late") {
          val slow      = Connector(bind = BindAddress.localhost(0), requestTimeoutMs = 400L)
          val h1Outcome = withH1(okRoutes, slow) { port =>
            ZIO.attemptBlocking {
              val socket = new Socket("127.0.0.1", port)
              socket.setSoTimeout(10000)
              try {
                socket.getOutputStream.write("GET /ok HTTP/1.1\r\n".getBytes(StandardCharsets.US_ASCII))
                socket.getOutputStream.flush()
                try {
                  val wire = new H1WireClient(socket.getInputStream, socket.getOutputStream)
                  val resp = wire.readResponse()
                  "status:" + resp.status.toString
                } catch {
                  case _: EOFException => "closed"
                }
              } finally socket.close()
            }
          }.map(outcome => assertTrue(outcome == "closed"))
          val h2Outcome = withH2c(okRoutes, slow) { port =>
            ZIO.attemptBlocking(h2StalledStreamForReset(port, "/ok"))
          }.map(outcome => assertTrue(outcome.startsWith("reset:")))
          val h1Healthy = withH1(okRoutes, slow) { port =>
            ZIO.attemptBlocking(new ParityCleartextH1Client(port).request("GET", "/ok", Nil, Chunk.empty).bodyText)
          }.map(body => assertTrue(body == "ok"))
          for {
            h1     <- h1Outcome
            h2     <- h2Outcome
            health <- h1Healthy
          } yield h1 && h2 && health
        },
        test("proxy: H1 passes forwarding headers untouched, H2 resolves them by trust policy") {
          // Intentional engine difference, pinned explicitly: the H1 engine is
          // transport-transparent (forwarding headers reach the handler
          // untouched; no synthetic trust headers), while the H2 engine
          // consumes X-Forwarded-*/Forwarded per TrustedProxyConfig — untrusted
          // peers see zero effect (headers stripped), trusted loopback peers
          // resolve client IP/host/scheme (surfaced as x-client-ip plus a
          // rewritten URL). H2ProxyTrustSpec owns the full matrix; this test
          // locks the cross-engine contract.
          val h1Echo          = withH1(proxyEchoRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(
              new ParityCleartextH1Client(port)
                .request(
                  "GET",
                  "/echo",
                  List(
                    "X-Forwarded-For"   -> "203.0.113.7",
                    "X-Forwarded-Proto" -> "https",
                    "X-Forwarded-Host"  -> "example.com",
                  ),
                  Chunk.empty,
                )
                .bodyText,
            )
          }
          val h2UntrustedEcho = withH2c(
            proxyEchoRoutes,
            Connector(bind = BindAddress.localhost(0), trustedProxy = TrustedProxyConfig()),
          ) { port =>
            ZIO.attemptBlocking(
              new H2ConformanceClient(port)
                .request(
                  "GET",
                  "/echo",
                  List(
                    "x-forwarded-for"   -> "203.0.113.7",
                    "x-forwarded-proto" -> "https",
                    "x-forwarded-host"  -> "example.com",
                  ),
                  Chunk.empty,
                )
                .bodyText,
            )
          }
          val h2TrustedEcho   = withH2c(
            proxyEchoRoutes,
            Connector(
              bind = BindAddress.localhost(0),
              trustedProxy = TrustedProxyConfig(trustedCidrs = Set("127.0.0.1/32")),
            ),
          ) { port =>
            ZIO.attemptBlocking(
              new H2ConformanceClient(port)
                .request(
                  "GET",
                  "/echo",
                  List(
                    "x-forwarded-for"   -> "203.0.113.7",
                    "x-forwarded-proto" -> "https",
                    "x-forwarded-host"  -> "example.com",
                  ),
                  Chunk.empty,
                )
                .bodyText,
            )
          }
          for {
            h1        <- h1Echo
            untrusted <- h2UntrustedEcho
            trusted   <- h2TrustedEcho
          } yield assertTrue(
            // H1 keeps the origin-form URL (no host/scheme synthesis) and
            // passes the forwarding header through untouched.
            h1 == "none|none|203.0.113.7|none|none|none",
            untrusted == "127.0.0.1|127.0.0.1|none|none|127.0.0.1|http",
            trusted == "203.0.113.7|127.0.0.1|none|none|example.com|https",
          )
        },
        test("access log: one record per request with matching metadata on H1 and H2C") {
          val echoRoutes: Routes[Any] = Routes(
            Route(
              RoutePattern(Method.GET, "/logged"),
              handler { (_: Request) =>
                responseAsResult(Response(status = Status.Ok, body = Body.fromString("logged-ok")))
              },
            ),
          )
          val h1Records               = withLoggedH1(echoRoutes) { case (port, sink) =>
            ZIO.attemptBlocking {
              val status = new ParityCleartextH1Client(port).request("GET", "/logged", Nil, Chunk.empty).status
              (status, sink.records)
            }
          }
          val h2Records               = withLoggedH2c(echoRoutes) { case (port, sink) =>
            ZIO.attemptBlocking {
              val status = new H2ConformanceClient(port).request("GET", "/logged", Nil, Chunk.empty).status
              (status, sink.records)
            }
          }
          for {
            h1Pair <- h1Records
            h2Pair <- h2Records
          } yield {
            val (h1Status, h1Recs) = h1Pair
            val (h2Status, h2Recs) = h2Pair
            assertTrue(
              h1Status == 200,
              h2Status == 200,
              h1Recs.length == 1,
              h2Recs.length == 1,
              h1Recs.head.method == "GET",
              h2Recs.head.method == "GET",
              h1Recs.head.path == "/logged",
              h2Recs.head.path == "/logged",
              h1Recs.head.status == 200,
              h2Recs.head.status == 200,
            )
          }
        },
        test("independent clients: JDK HTTP/1.1 cleartext and JDK HTTP/2 TLS agree byte-exact") {
          val h1Outcome = withH1(okRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking {
              val client   = java.net.http.HttpClient
                .newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1)
                .connectTimeout(java.time.Duration.ofSeconds(5))
                .build()
              val request  = java.net.http.HttpRequest
                .newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/ok"))
                .timeout(java.time.Duration.ofSeconds(10))
                .GET()
                .build()
              val response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
              (response.version(), response.statusCode(), response.body())
            }
          }.map { case (version, status, body) =>
            assertTrue(version == java.net.http.HttpClient.Version.HTTP_1_1, status == 200, body == "ok")
          }
          val h2Outcome = withSharedTls(okRoutes) { port =>
            ZIO.attemptBlocking {
              val client                  = TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)
              val (version, status, body) = TlsAlpnFixtures.jdkGet(client, port, "/ok")
              (version, status, body)
            }
          }.map { case (version, status, body) =>
            assertTrue(version == java.net.http.HttpClient.Version.HTTP_2, status == 200, body == "ok")
          }
          for {
            h1 <- h1Outcome
            h2 <- h2Outcome
          } yield h1 && h2
        },
        test("SSE: paced events stay byte-exact with delay preserved on H1, H2C and shared TLS") {
          val expected   = (0 until 3)
            .map(i => new String(SseCodec.encode(ServerSentEvent("tick" + i)).toArray, StandardCharsets.UTF_8))
            .mkString
          val h1Elapsed  = withH1(pacedSseRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking {
              val start = java.lang.System.nanoTime()
              val body  = new ParityCleartextH1Client(port).request("GET", "/sse-paced", Nil, Chunk.empty).bodyText
              ((java.lang.System.nanoTime() - start) / 1000000L, body)
            }
          }
          val h2Elapsed  = withH2c(pacedSseRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking {
              val start = java.lang.System.nanoTime()
              val body  = new H2ConformanceClient(port).request("GET", "/sse-paced", Nil, Chunk.empty).bodyText
              ((java.lang.System.nanoTime() - start) / 1000000L, body)
            }
          }
          val tlsElapsed = withSharedTls(pacedSseRoutes) { port =>
            ZIO.attemptBlocking {
              val start = java.lang.System.nanoTime()
              val body  = new ParityJdkH2Client(port).request("GET", "/sse-paced", Nil, Chunk.empty).bodyText
              ((java.lang.System.nanoTime() - start) / 1000000L, body)
            }
          }
          h1Elapsed.map { case (h1Ms, h1Body) => (h1Ms, h1Body) }.flatMap { case (h1Ms, h1Body) =>
            h2Elapsed.flatMap { case (h2Ms, h2Body) =>
              tlsElapsed.map { case (tlsMs, tlsBody) =>
                assertTrue(
                  h1Body == expected,
                  h2Body == expected,
                  tlsBody == expected,
                  h1Ms >= 300L,
                  h2Ms >= 300L,
                  tlsMs >= 300L,
                )
              }
            }
          }
        },
        test("session: two logins mint distinct tokens with per-token isolation on H1 and H2C") {
          val h1Outcome = withH1(ConformanceCorpus.routes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(sessionIsolationProbe(new ParityCleartextH1Client(port)))
          }.map(probe => assertTrue(probe))
          val h2Outcome = withH2c(ConformanceCorpus.routes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(sessionIsolationProbe(new H2ConformanceClient(port)))
          }.map(probe => assertTrue(probe))
          for {
            h1 <- h1Outcome
            h2 <- h2Outcome
          } yield h1 && h2
        },
        test("endpoint: JSON route is byte-identical on H1, H2C and both shared ports") {
          val jsonRoutes: Routes[Any] = Routes(
            Route(
              RoutePattern(Method.GET, "/api/greet"),
              handler { (_: Request) =>
                responseAsResult(
                  Response(status = Status.Ok, body = Body.fromString("{\"hello\":\"world\"}"))
                    .addHeader(Header.Custom("Content-Type", "application/json")),
                )
              },
            ),
          )
          val h1Resp                  = withH1(jsonRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(new ParityCleartextH1Client(port).request("GET", "/api/greet", Nil, Chunk.empty))
          }
          val h2Resp                  = withH2c(jsonRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(new H2ConformanceClient(port).request("GET", "/api/greet", Nil, Chunk.empty))
          }
          val tlsResp                 = withSharedTls(jsonRoutes) { port =>
            ZIO.attemptBlocking(new ParityJdkH2Client(port).request("GET", "/api/greet", Nil, Chunk.empty))
          }
          val sharedResp              = withSharedCleartext(jsonRoutes) { port =>
            ZIO.attemptBlocking(new ParityCleartextH1Client(port).request("GET", "/api/greet", Nil, Chunk.empty))
          }
          for {
            first  <- h1Resp
            second <- h2Resp
            third  <- tlsResp
            fourth <- sharedResp
          } yield assertTrue(
            first.status == 200,
            second.status == 200,
            third.status == 200,
            fourth.status == 200,
            first.bodyText == "{\"hello\":\"world\"}",
            second.bodyText == first.bodyText,
            third.bodyText == first.bodyText,
            fourth.bodyText == first.bodyText,
            second.headerFirst("content-type").exists(_.contains("application/json")),
            third.headerFirst("content-type").exists(_.contains("application/json")),
          )
        },
        test("upgrade: h2c Upgrade on H1 is ignored with plain 200 on single and shared ports") {
          val upgradeHeaders =
            "Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n"
          val single         = withH1(okRoutes, Connector(bind = BindAddress.localhost(0))) { port =>
            ZIO.attemptBlocking(rawH1Get(port, "/ok", upgradeHeaders))
          }.map { case (status, body) => assertTrue(status == 200, body == "ok") }
          val shared         = withSharedCleartext(okRoutes) { port =>
            ZIO.attemptBlocking(rawH1Get(port, "/ok", upgradeHeaders))
          }.map { case (status, body) => assertTrue(status == 200, body == "ok") }
          for {
            first  <- single
            second <- shared
          } yield first && second
        },
      ),
    ) @@ sequential

  private val okRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/ok"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("ok")))
      },
    ),
  )

  private val proxyEchoRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/echo"),
      handler { (req: Request) =>
        val clientIp        = req.headers.rawGet("x-client-ip").getOrElse("none")
        val peer            = req.headers.rawGet("x-peer-address").getOrElse("none")
        val forwardedFor    = req.headers.rawGet("x-forwarded-for").getOrElse("none")
        val forwardedHeader = req.headers.rawGet("forwarded").getOrElse("none")
        val host            = req.url.host.getOrElse("none")
        val scheme          = req.url.scheme.map(_.text).getOrElse("none")
        responseAsResult(
          Response(
            status = Status.Ok,
            body = Body.fromString(s"$clientIp|$peer|$forwardedFor|$forwardedHeader|$host|$scheme"),
          ),
        )
      },
    ),
  )

  private val pacedSseRoutes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/sse-paced"),
      handler { (_: Request) =>
        // Handler-side pacing on a Loom virtual thread (the SseDelayIntegrationSpec
        // convention): each event parks its stream thread for 200ms, so a
        // batching transport would deliver all three at once while a preserving
        // one spaces them ~200ms apart. No test-side sleeps anywhere.
        val paced = Stream.unfold(0) { i =>
          if (i >= 3) None
          else {
            Thread.sleep(200L)
            Some((ServerSentEvent("tick" + i), i + 1))
          }
        }
        responseAsResult(Sse.response(paced))
      },
    ),
  )

  private def withH1[R, A](
    routes: Routes[Any],
    connector: Connector,
  )(use: Int => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val engine = new H1Transport(routes, Context.empty, connector, DefectHandler.default)
          val server = LoomServer(connector).withEngine(engine)
          Server.serve(routes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap(portOf(_, use))

  private def withH2c[R, A](
    routes: Routes[Any],
    connector: Connector,
  )(use: Int => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val engine = new H2Engine(routes, Context.empty, connector, DefectHandler.default)
          val server = LoomServer(connector).withEngine(engine)
          Server.serve(routes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap(portOf(_, use))

  private def withSharedTls[R, A](
    routes: Routes[Any],
  )(use: Int => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val connector =
            Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig()))
          val engine    = new H1H2TlsEngine(routes, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          Server.serve(routes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap(portOf(_, use))

  private def withSharedCleartext[R, A](
    routes: Routes[Any],
  )(use: Int => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val connector = Connector(
            bind = BindAddress.localhost(0),
            protocol = Protocol.H2C(),
            negotiation = NegotiationPolicy.CleartextPreface,
          )
          val engine    = new H1H2CleartextEngine(routes, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          Server.serve(routes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap(portOf(_, use))

  private def portOf[R, A](
    handle: zio.http.ServerHandle,
    use: Int => ZIO[R, Throwable, A],
  ): ZIO[R, Throwable, A] = {
    val port = handle.bindings.head.address match {
      case BoundAddress.Tcp(_, value) => value
      case other                      => throw new AssertionError("Expected TCP binding but found: " + other)
    }
    use(port)
  }

  private def withLoggedH1[R, A](
    routes: Routes[Any],
  )(use: ((Int, CaptureSink)) => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] = {
    val sink = CaptureSink()
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val connector = Connector(bind = BindAddress.localhost(0))
          val logged    = routes @@ Middleware.accessLog(sink)
          val engine    = new H1Transport(logged, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          Server.serve(logged, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, value) => value
          case other                      => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use((port, sink))
      }
  }

  private def withLoggedH2c[R, A](
    routes: Routes[Any],
  )(use: ((Int, CaptureSink)) => ZIO[R, Throwable, A]): ZIO[R with Scope, Throwable, A] = {
    val sink = CaptureSink()
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val connector = Connector(bind = BindAddress.localhost(0))
          val logged    = routes @@ Middleware.accessLog(sink)
          val engine    = new H2Engine(logged, Context.empty, connector, DefectHandler.default)
          val server    = LoomServer(connector).withEngine(engine)
          Server.serve(logged, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, value) => value
          case other                      => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use((port, sink))
      }
  }

  private final class CaptureSink extends AccessLogSink {
    private val queue                      = new java.util.concurrent.ConcurrentLinkedQueue[AccessLogRecord]()
    def log(record: AccessLogRecord): Unit = { queue.add(record); () }
    def records: List[AccessLogRecord]     = queue.toArray(new Array[AccessLogRecord](0)).toList
  }

  private object CaptureSink {
    def apply(): CaptureSink = new CaptureSink()
  }

  /** Two logins mint distinct tokens; each token sees only itself. */
  private def sessionIsolationProbe(client: ConformanceClient): Boolean = {
    def tokenOf(login: ObservedResponse): String =
      login
        .headerFirst("set-cookie")
        .getOrElse("")
        .split(";")
        .map(_.trim)
        .find(_.startsWith("session="))
        .map(_.substring("session=".length))
        .getOrElse("")
    val first                                    = client.request("POST", "/session/login", Nil, Chunk.empty)
    val second                                   = client.request("POST", "/session/login", Nil, Chunk.empty)
    val t1                                       = tokenOf(first)
    val t2                                       = tokenOf(second)
    val me1   = client.request("GET", "/session/me", List("Cookie" -> ("session=" + t1)), Chunk.empty)
    val me2   = client.request("GET", "/session/me", List("Cookie" -> ("session=" + t2)), Chunk.empty)
    val cross = client.request("GET", "/session/me", List("Cookie" -> ("session=" + t1 + "x")), Chunk.empty)
    first.status == 200 && second.status == 200 &&
    t1.length == 43 && t2.length == 43 && t1 != t2 &&
    me1.status == 200 && me1.bodyText == "me:" + t1 &&
    me2.status == 200 && me2.bodyText == "me:" + t2 &&
    cross.status == 403
  }

  /** Raw H1 GET with extra headers; returns (status, body). */
  private def rawH1Get(port: Int, target: String, extraHeaders: String): (Int, String) = {
    val socket = new Socket("127.0.0.1", port)
    socket.setSoTimeout(10000)
    try {
      val out  = socket.getOutputStream
      out.write(
        ("GET " + target + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n" + extraHeaders + "\r\n")
          .getBytes(StandardCharsets.US_ASCII),
      )
      out.flush()
      val wire = new H1WireClient(socket.getInputStream, out)
      val resp = wire.readResponse()
      (resp.status, resp.bodyText)
    } finally socket.close()
  }

  /**
   * Raw H2C POST carrying `body`; reports "reset:<code>" when the stream is
   * reset, "status:<n>" when a full response arrives, or "timeout"/"closed". A
   * 200 here is the failure mode (an over-cap body must never echo).
   */
  private def h2PostForReset(port: Int, path: String, body: Chunk[Byte]): String = {
    val socket = new Socket("127.0.0.1", port)
    socket.setSoTimeout(8000)
    try {
      val conn = new ResetProbeConnection(socket, port)
      conn.handshake()
      conn.post(path, body, endStream = true)
      conn.awaitReset(streamId = 1)
    } catch {
      case _: java.net.SocketTimeoutException => "timeout"
      case _: EOFException                    => "closed"
    } finally socket.close()
  }

  /**
   * Raw H2C GET with headers sent but the stream left half-open; the
   * request-timeout path must reset it. Same outcome vocabulary as
   * [[h2PostForReset]].
   */
  private def h2StalledStreamForReset(port: Int, path: String): String = {
    val socket = new Socket("127.0.0.1", port)
    socket.setSoTimeout(8000)
    try {
      val conn = new ResetProbeConnection(socket, port)
      conn.handshake()
      conn.stalledGet(path)
      conn.awaitReset(streamId = 1)
    } catch {
      case _: java.net.SocketTimeoutException => "timeout"
      case _: EOFException                    => "closed"
    } finally socket.close()
  }

  private final class ResetProbeConnection(socket: Socket, port: Int) {
    private val UsAscii = StandardCharsets.US_ASCII
    private val Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(UsAscii)
    private val out     = socket.getOutputStream
    private val in      = socket.getInputStream
    private var buf     = Chunk.empty[Byte]
    private val encoder = new HpackEncoder()
    private val decoder = new HpackDecoder()

    def handshake(): Unit = {
      out.write(Preface)
      out.write(FrameCodec.encode(Settings(ack = false, Nil)).toArray)
      out.flush()
      readFrame() match {
        case Settings(false, _) => sendFrame(Settings(ack = true, Nil))
        case other              => throw new AssertionError("Expected server SETTINGS but got: " + other)
      }
      readFrame()
    }

    def post(path: String, body: Chunk[Byte], endStream: Boolean): Unit = {
      val pseudo = List(
        HeaderField(":method", "POST"),
        HeaderField(":path", path),
        HeaderField(":scheme", "http"),
        HeaderField(":authority", "127.0.0.1:" + port),
      )
      sendFrame(Headers(streamId = 1, headerBlock = encoder.encode(pseudo), endStream = false, endHeaders = true))
      val bytes  = body.toArray
      var offset = 0
      while (offset < bytes.length) {
        val end   = Math.min(offset + 16384, bytes.length)
        val slice = Chunk.fromArray(java.util.Arrays.copyOfRange(bytes, offset, end))
        sendFrame(Data(streamId = 1, data = slice, endStream = endStream && end == bytes.length))
        offset = end
      }
    }

    def stalledGet(path: String): Unit = {
      val pseudo = List(
        HeaderField(":method", "GET"),
        HeaderField(":path", path),
        HeaderField(":scheme", "http"),
        HeaderField(":authority", "127.0.0.1:" + port),
      )
      // Headers without END_STREAM and no body: the stream stays half-open
      // until the server request timer fires.
      sendFrame(Headers(streamId = 1, headerBlock = encoder.encode(pseudo), endStream = false, endHeaders = true))
    }

    def awaitReset(streamId: Int): String = {
      var outcome = "timeout"
      var waiting = true
      while (waiting) {
        readFrame() match {
          case Settings(false, _)                                   => sendFrame(Settings(ack = true, Nil))
          case Settings(true, _)                                    => ()
          case _: WindowUpdate                                      => ()
          case Ping(false, data)                                    => sendFrame(Ping(ack = true, data))
          case RstStream(sid, code) if sid == streamId              =>
            outcome = "reset:" + code.value.toString
            waiting = false
          case Headers(sid, block, end, _, _, _) if sid == streamId =>
            val fields = decode(block)
            val status = fields.find(_.name == ":status").map(_.value).getOrElse("?")
            if (end) {
              outcome = "status:" + status
              waiting = false
            }
          case Data(sid, data, end, _) if sid == streamId           =>
            topUp(sid, data.length)
            if (end) {
              outcome = "status:200-data"
              waiting = false
            }
          case Data(sid, data, _, _)                                =>
            topUp(sid, data.length)
          case GoAway(_, code, _)                                   =>
            outcome = "goaway:" + code.value.toString
            waiting = false
          case _                                                    => ()
        }
      }
      outcome
    }

    private def decode(block: Chunk[Byte]): List[HeaderField] =
      decoder.decode(block) match {
        case Right(fields) => fields
        case Left(error)   => throw new AssertionError("HPACK decode failed: " + error)
      }

    private def topUp(streamId: Int, bytes: Int): Unit =
      if (bytes > 0) {
        sendFrame(WindowUpdate(streamId = 0, increment = bytes))
        sendFrame(WindowUpdate(streamId = streamId, increment = bytes))
      }

    private def sendFrame(frame: H2Frame): Unit = {
      out.write(FrameCodec.encode(frame).toArray)
      out.flush()
    }

    private def readFrame(): H2Frame =
      FrameCodec.decode(buf) match {
        case Right((frame, rest))           =>
          buf = rest
          frame
        case Left(H2Error.InsufficientData) =>
          val tmp = new Array[Byte](8192)
          val n   = in.read(tmp)
          if (n < 0) throw new EOFException("Connection closed mid-frame")
          buf = buf ++ Chunk.fromArray(java.util.Arrays.copyOf(tmp, n))
          readFrame()
        case Left(error)                    =>
          throw new AssertionError("Frame decode failed: " + error)
      }
  }
}

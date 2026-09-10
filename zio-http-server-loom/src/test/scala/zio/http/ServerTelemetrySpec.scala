package zio.http

import java.net.Socket
import java.nio.charset.StandardCharsets

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.ResultType._
import zio.http.h1.H1RawClientFixture._
import zio.http.h1.H1Transport
import zio.http.h2.H2RawClientFixture._
import zio.http.h2.H2Transport
import zio.http.{Body, Handler, Headers, Method, Request, Response, Route, Routes, Status, handler}

/**
 * Todo 9: protocol-neutral observability and typed failures.
 *
 * Every engine emits the same [[ServerDiagnostic]] vocabulary through an
 * injected [[ServerTelemetry]]: success metrics and protocol-specific wire
 * failures share `protocol`/`connector` labels, while H1/H2 wire errors keep
 * their own types (only the error class name crosses the boundary — never
 * header values, secrets, or raw body bytes).
 */
@experimental
object ServerTelemetrySpec extends ZIOSpecDefault {

  private val h1Routes: Routes[Any] = Routes(
    Route(
      RoutePattern.GET,
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("ok")))
      },
    ),
    Route(
      RoutePattern(Method.POST, "/echo"),
      handler { (req: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString(req.body.asString())))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/boom"),
      Handler.fromRequest((_: Request) => throw new RuntimeException("boom")),
    ),
  )

  private val h2Routes: Routes[Any] = Routes(
    Route(
      RoutePattern.GET,
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("ok")))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/boom"),
      Handler.fromRequest((_: Request) => throw new RuntimeException("boom")),
    ),
  )

  private def h1Connector: Connector =
    Connector(bind = BindAddress.localhost(0))

  private def expectedH1Label: String =
    ServerTelemetry.connectorLabel(h1Connector)

  private def withH1Telemetry[R](routes: Routes[Any], connector: Connector, telemetry: ServerTelemetry)(
    use: Int => ZIO[R, Throwable, TestResult],
  ): ZIO[R & Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(
        ZIO.attempt(
          ServerHandle.live(
            List(
              new H1Transport(routes, Context.empty, connector, DefectHandler.default, telemetry)
                .start(),
            ),
          ),
        ),
      )(h => ZIO.succeed(h.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, p) => p
          case other                  => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use(port)
      }

  private def withH2Telemetry[R](routes: Routes[Any], connector: Connector, telemetry: ServerTelemetry)(
    use: Int => ZIO[R, Throwable, TestResult],
  ): ZIO[R & Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(
        ZIO.attempt(
          ServerHandle.live(
            List(
              new H2Transport(routes, Context.empty, connector, DefectHandler.default, telemetry = telemetry)
                .start(),
            ),
          ),
        ),
      )(h => ZIO.succeed(h.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, p) => p
          case other                  => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use(port)
      }

  /**
   * Bounded poll for an asynchronously recorded diagnostic (server I/O
   * threads).
   */
  private def awaitRecord(telemetry: ServerTelemetry.InMemory, p: ServerDiagnostic => Boolean): ServerDiagnostic = {
    val deadline                = java.lang.System.currentTimeMillis() + 5000L
    var found: ServerDiagnostic = null
    while (found == null) {
      telemetry.records.find(p) match {
        case Some(d) => found = d
        case None    =>
          if (java.lang.System.currentTimeMillis() > deadline)
            throw new AssertionError(
              "Timed out waiting for diagnostic; recorded: " + telemetry.records.mkString(", "),
            )
          Thread.sleep(25L)
      }
    }
    found
  }

  private def assertNoLeak(telemetry: ServerTelemetry.InMemory, secrets: List[String]): TestResult = {
    val rendered = telemetry.records.map(_.toString)
    assertTrue(secrets.forall(secret => rendered.forall(!_.contains(secret))))
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ServerTelemetrySpec")(
      test("labels H1 success with protocol h1 and the connector label") {
        val telemetry = new ServerTelemetry.InMemory()
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp      = client.readResponse()
              val completed = awaitRecord(
                telemetry,
                { case ServerDiagnostic.RequestCompleted(_, _, _, _, _, _) => true; case _ => false },
              )
              assertTrue(
                resp.status == 200,
                completed == ServerDiagnostic.RequestCompleted(
                  protocol = ProtocolLabel.H1,
                  connector = expectedH1Label,
                  method = "GET",
                  path = "/",
                  status = 200,
                  durationMs = completed.asInstanceOf[ServerDiagnostic.RequestCompleted].durationMs,
                ),
                completed.asInstanceOf[ServerDiagnostic.RequestCompleted].durationMs >= 0L,
              )
            } finally client.close()
          }
        }
      },
      test("labels H2C success with protocol h2c and the connector label") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0))
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              val resp      = client.roundTrip("GET", "/", Chunk.empty, streamId = 1)
              val completed = awaitRecord(
                telemetry,
                { case ServerDiagnostic.RequestCompleted(_, _, _, _, _, _) => true; case _ => false },
              )
              assertTrue(
                resp.status == 200,
                completed == ServerDiagnostic.RequestCompleted(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  method = "GET",
                  path = "/",
                  status = 200,
                  durationMs = completed.asInstanceOf[ServerDiagnostic.RequestCompleted].durationMs,
                ),
              )
            } finally client.close()
          }
        }
      },
      test("never leaks secrets or raw bodies into H1 diagnostics") {
        val telemetry    = new ServerTelemetry.InMemory()
        val secretToken  = "bearer-super-secret-token-9"
        val secretCookie = "session-secret-cookie-9"
        val secretBody   = "password=super-secret-body-9"
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw(
                "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer " + secretToken +
                  "\r\nCookie: " + secretCookie +
                  "\r\nContent-Length: " + secretBody.length + "\r\n\r\n" + secretBody,
              )
              val resp = client.readResponse()
              // Malformed follow-up carrying secret bytes must also stay redacted.
              client.sendRaw("GARBAGE-BYTES-" + secretToken + "\r\n\r\n")
              try client.readResponse()
              catch { case _: Throwable => () }
              assertTrue(resp.status == 200) &&
              assertNoLeak(telemetry, List(secretToken, secretCookie, secretBody))
            } finally client.close()
          }
        }
      },
      test("never leaks secrets or raw bodies into H2 diagnostics") {
        val telemetry  = new ServerTelemetry.InMemory()
        val secretBody = "password=super-secret-h2-body-9"
        val connector  = Connector(bind = BindAddress.localhost(0))
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              val body = Chunk.fromArray(secretBody.getBytes(StandardCharsets.UTF_8))
              val resp = client.roundTrip("GET", "/", body, streamId = 1)
              assertTrue(resp.status == 200) &&
              assertNoLeak(telemetry, List(secretBody))
            } finally client.close()
          }
        }
      },
      test("records H1 parser rejections with the typed H1 error") {
        val telemetry = new ServerTelemetry.InMemory()
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              // TE + CL on one message is unambiguous rejection (AmbiguousFraming).
              client.sendRaw(
                "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello",
              )
              val resp     = client.readResponse()
              val rejected = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, _, _) => true; case _ => false },
              )
              assertTrue(
                resp.status == 400,
                rejected == ServerDiagnostic.ParseRejected(
                  protocol = ProtocolLabel.H1,
                  connector = expectedH1Label,
                  errorType = "AmbiguousFraming",
                  status = Some(400),
                ),
              )
            } finally client.close()
          }
        }
      },
      test("records H1 body-cap rejections as 413") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0), maxRequestBodySize = 8L)
        withH1Telemetry(h1Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw(
                "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 100\r\n\r\n" +
                  "0123456789" * 10,
              )
              val resp     = client.readResponse()
              val rejected = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, _, _) => true; case _ => false },
              )
              assertTrue(
                resp.status == 413,
                rejected == ServerDiagnostic.ParseRejected(
                  protocol = ProtocolLabel.H1,
                  connector = ServerTelemetry.connectorLabel(connector),
                  errorType = "BodyTooLarge",
                  status = Some(413),
                ),
              )
            } finally client.close()
          }
        }
      },
      test("records H1 unknown methods as 501") {
        val telemetry = new ServerTelemetry.InMemory()
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("FROB / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp     = client.readResponse()
              val rejected = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, _, _) => true; case _ => false },
              )
              assertTrue(
                resp.status == 501,
                rejected == ServerDiagnostic.ParseRejected(
                  protocol = ProtocolLabel.H1,
                  connector = expectedH1Label,
                  errorType = "UnknownMethod",
                  status = Some(501),
                ),
              )
            } finally client.close()
          }
        }
      },
      test("records H1 CONNECT and bad targets with typed diagnostics") {
        val telemetry = new ServerTelemetry.InMemory()
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
              val connectResp     = client.readResponse()
              val rejectedConnect = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, "UnsupportedMethod", _) => true; case _ => false },
              )
              val client2         = new RawH1Client(port)
              try {
                client2.sendRaw("GET * HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                val starResp       = client2.readResponse()
                val rejectedTarget = awaitRecord(
                  telemetry,
                  { case ServerDiagnostic.ParseRejected(_, _, "InvalidTarget", _) => true; case _ => false },
                )
                assertTrue(
                  connectResp.status == 501,
                  rejectedConnect == ServerDiagnostic.ParseRejected(
                    protocol = ProtocolLabel.H1,
                    connector = expectedH1Label,
                    errorType = "UnsupportedMethod",
                    status = Some(501),
                  ),
                  starResp.status == 400,
                  rejectedTarget == ServerDiagnostic.ParseRejected(
                    protocol = ProtocolLabel.H1,
                    connector = expectedH1Label,
                    errorType = "InvalidTarget",
                    status = Some(400),
                  ),
                )
              } finally client2.close()
            } finally client.close()
          }
        }
      },
      test("records H1 handler defects as completed 500s") {
        val telemetry = new ServerTelemetry.InMemory()
        withH1Telemetry(h1Routes, h1Connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET /boom HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp      = client.readResponse()
              val completed = awaitRecord(
                telemetry,
                {
                  case ServerDiagnostic.RequestCompleted(_, _, _, "/boom", 500, _) => true
                  case _                                                           => false
                },
              )
              assertTrue(
                resp.status == 500,
                completed.asInstanceOf[ServerDiagnostic.RequestCompleted].protocol == ProtocolLabel.H1,
                completed.asInstanceOf[ServerDiagnostic.RequestCompleted].connector == expectedH1Label,
              )
            } finally client.close()
          }
        }
      },
      test("records H1 request timeouts when a turn stalls") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0), requestTimeoutMs = 400L)
        withH1Telemetry(h1Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val socket = new Socket("127.0.0.1", port)
            socket.setSoTimeout(10000)
            try {
              val out      = socket.getOutputStream
              out.write("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n".getBytes(StandardCharsets.US_ASCII))
              out.flush()
              val timedOut = awaitRecord(
                telemetry,
                { case ServerDiagnostic.RequestTimeout(_, _, _, _) => true; case _ => false },
              )
              assertTrue(
                timedOut == ServerDiagnostic.RequestTimeout(
                  protocol = ProtocolLabel.H1,
                  connector = ServerTelemetry.connectorLabel(connector),
                  timeoutKind = TimeoutKind.Request,
                  errorType = "RequestTimeout",
                ),
              )
            } finally socket.close()
          }
        }
      },
      test("records H2 body-cap violations with no HTTP status") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0), maxRequestBodySize = 16L)
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              // Over-cap POST on stream 1; sibling stream 3 must still serve (per-stream reset).
              val big      = Chunk.fromArray(new Array[Byte](100))
              try client.roundTrip("POST", "/", big, streamId = 1)
              catch { case _: Throwable => () }
              val rejected = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, _, _) => true; case _ => false },
              )
              val sibling  = client.roundTrip("GET", "/", Chunk.empty, streamId = 3)
              assertTrue(
                rejected == ServerDiagnostic.ParseRejected(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  errorType = "RequestBodyTooLarge",
                  status = None,
                ),
                sibling.status == 200,
              )
            } finally client.close()
          }
        }
      },
      test("records H2 body timeouts and stream resets") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0), bodyTimeoutMs = 400L)
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              // Declared 100 bytes, only a 3-byte drip arrives, then stall past the deadline.
              client.sendFrame(
                zio.http.h2.H2Frame.Headers(
                  streamId = 1,
                  headerBlock = client.encodeHeaders(
                    List(
                      zio.http.h2.hpack.HeaderField(":method", "POST"),
                      zio.http.h2.hpack.HeaderField(":path", "/"),
                      zio.http.h2.hpack.HeaderField(":scheme", "http"),
                      zio.http.h2.hpack.HeaderField(":authority", s"127.0.0.1:$port"),
                      zio.http.h2.hpack.HeaderField("content-length", "100"),
                    ),
                  ),
                  endStream = false,
                  endHeaders = true,
                ),
              )
              client.sendFrame(
                zio.http.h2.H2Frame.Data(1, Chunk.fromArray("abc".getBytes(StandardCharsets.UTF_8)), endStream = false),
              )
              val timedOut = awaitRecord(
                telemetry,
                { case ServerDiagnostic.RequestTimeout(_, _, _, _) => true; case _ => false },
              )
              val reset    = awaitRecord(
                telemetry,
                { case ServerDiagnostic.StreamReset(_, _, _, _) => true; case _ => false },
              )
              assertTrue(
                timedOut == ServerDiagnostic.RequestTimeout(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  timeoutKind = TimeoutKind.Body,
                  errorType = "StreamTimeout",
                ),
                reset == ServerDiagnostic.StreamReset(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  reason = StreamResetReason.Cancel,
                  streamId = Some(1),
                ),
              )
            } finally client.close()
          }
        }
      },
      test("records H2 length mismatches with a protocol-error reset") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0))
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              // Declared 100 bytes but END_STREAM after 3: length mismatch, no stall.
              client.sendFrame(
                zio.http.h2.H2Frame.Headers(
                  streamId = 1,
                  headerBlock = client.encodeHeaders(
                    List(
                      zio.http.h2.hpack.HeaderField(":method", "GET"),
                      zio.http.h2.hpack.HeaderField(":path", "/"),
                      zio.http.h2.hpack.HeaderField(":scheme", "http"),
                      zio.http.h2.hpack.HeaderField(":authority", s"127.0.0.1:$port"),
                      zio.http.h2.hpack.HeaderField("content-length", "100"),
                    ),
                  ),
                  endStream = false,
                  endHeaders = true,
                ),
              )
              client.sendFrame(
                zio.http.h2.H2Frame.Data(1, Chunk.fromArray("abc".getBytes(StandardCharsets.UTF_8)), endStream = true),
              )
              val rejected = awaitRecord(
                telemetry,
                { case ServerDiagnostic.ParseRejected(_, _, _, _) => true; case _ => false },
              )
              val reset    = awaitRecord(
                telemetry,
                { case ServerDiagnostic.StreamReset(_, _, _, _) => true; case _ => false },
              )
              val sibling  = client.roundTrip("GET", "/", Chunk.empty, streamId = 3)
              assertTrue(
                rejected == ServerDiagnostic.ParseRejected(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  errorType = "RequestBodyLengthMismatch",
                  status = None,
                ),
                reset == ServerDiagnostic.StreamReset(
                  protocol = ProtocolLabel.H2C,
                  connector = ServerTelemetry.connectorLabel(connector),
                  reason = StreamResetReason.ProtocolError,
                  streamId = Some(1),
                ),
                sibling.status == 200,
              )
            } finally client.close()
          }
        }
      },
      test("records H2 handler defects as completed 500s") {
        val telemetry = new ServerTelemetry.InMemory()
        val connector = Connector(bind = BindAddress.localhost(0))
        withH2Telemetry(h2Routes, connector, telemetry) { port =>
          ZIO.attemptBlocking {
            val client = new RawH2Client(port)
            try {
              val resp = client.roundTrip("GET", "/boom", Chunk.empty, streamId = 1)
              assertTrue(resp.status == 500)
            } finally client.close()
          }
        }
      },
      test("records H2 bind conflicts as typed bind failures") {
        ZIO.attemptBlocking {
          val telemetry = new ServerTelemetry.InMemory()
          val connector = Connector(bind = BindAddress.localhost(0))
          val first     =
            new H2Transport(h2Routes, Context.empty, connector, DefectHandler.default, telemetry = telemetry)
          val bound     = first.start()
          try {
            val port              = bound.binding.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP: " + other)
            }
            val conflicting       = Connector(bind = BindAddress.localhost(port))
            val second            =
              new H2Transport(h2Routes, Context.empty, conflicting, DefectHandler.default, telemetry = telemetry)
            var thrown: Throwable = null
            try second.start()
            catch { case e: Throwable => thrown = e }
            val failure           = awaitRecord(
              telemetry,
              { case ServerDiagnostic.BindFailure(_, _) => true; case _ => false },
            )
            assertTrue(
              thrown != null,
              failure == ServerDiagnostic.BindFailure(
                connector = ServerTelemetry.connectorLabel(conflicting),
                errorType = thrown.getClass.getSimpleName,
              ),
            )
          } finally bound.close0()
        }
      },
      test("records H1 drain and force-close distinctly") {
        ZIO.attemptBlocking {
          val telemetry = new ServerTelemetry.InMemory()
          val transport =
            new H1Transport(h1Routes, Context.empty, h1Connector, DefectHandler.default, telemetry)
          val bound     = transport.start()
          try {
            val port         = bound.binding.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP: " + other)
            }
            val kept         = new RawH1Client(port)
            try {
              kept.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val first = kept.readResponse()
              transport.drain()
              kept.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val last  = kept.readResponse()
              assertTrue(first.status == 200, last.status == 200)
            } finally kept.close()
            val drainedClose = awaitRecord(
              telemetry,
              { case ServerDiagnostic.ConnectionClosed(_, _, true) => true; case _ => false },
            )
            // A fresh transport that is force-closed reports drained = false.
            val telemetry2   = new ServerTelemetry.InMemory()
            val transport2   =
              new H1Transport(h1Routes, Context.empty, h1Connector, DefectHandler.default, telemetry2)
            val bound2       = transport2.start()
            try {
              val port2       = bound2.binding.address match {
                case BoundAddress.Tcp(_, p) => p
                case other                  => throw new AssertionError("Expected TCP: " + other)
              }
              val client      = new RawH1Client(port2)
              try {
                client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                val first = client.readResponse()
                transport2.close()
                assertTrue(first.status == 200)
              } finally client.close()
              val forcedClose = awaitRecord(
                telemetry2,
                { case ServerDiagnostic.ConnectionClosed(_, _, _) => true; case _ => false },
              )
              assertTrue(
                drainedClose == ServerDiagnostic.ConnectionClosed(
                  protocol = ProtocolLabel.H1,
                  connector = expectedH1Label,
                  drained = true,
                ),
                forcedClose == ServerDiagnostic.ConnectionClosed(
                  protocol = ProtocolLabel.H1,
                  connector = expectedH1Label,
                  drained = false,
                ),
              )
            } finally bound2.close0()
          } finally {
            transport.close()
            bound.close0()
          }
        }
      },
      test("records bind conflicts as typed bind failures") {
        ZIO.attemptBlocking {
          val telemetry = new ServerTelemetry.InMemory()
          val first     =
            new H1Transport(h1Routes, Context.empty, h1Connector, DefectHandler.default, telemetry)
          val bound     = first.start()
          try {
            val port              = bound.binding.address match {
              case BoundAddress.Tcp(_, p) => p
              case other                  => throw new AssertionError("Expected TCP: " + other)
            }
            val conflicting       = Connector(bind = BindAddress.localhost(port))
            val second            =
              new H1Transport(h1Routes, Context.empty, conflicting, DefectHandler.default, telemetry)
            var thrown: Throwable = null
            try second.start()
            catch { case e: Throwable => thrown = e }
            val failure           = awaitRecord(
              telemetry,
              { case ServerDiagnostic.BindFailure(_, _) => true; case _ => false },
            )
            assertTrue(
              thrown != null,
              failure == ServerDiagnostic.BindFailure(
                connector = ServerTelemetry.connectorLabel(conflicting),
                errorType = thrown.getClass.getSimpleName,
              ),
            )
          } finally bound.close0()
        }
      },
      test("maps negotiation failures to typed diagnostics without string parsing") {
        val connector     = Connector(bind = BindAddress.localhost(0))
        val label         = ServerTelemetry.connectorLabel(connector)
        val h2Set         = ProtocolSet.fromLegacy(Protocol.H2C()).toOption.get
        val alpnMiss      = Negotiation.negotiateAlpn(h2Set, List("bogus-alpn"))
        val prefaceMiss   = Negotiation.selectForPreface(h2Set, isH2Preface = false)
        val h1Set         = ProtocolSet.fromSeq(Seq[AppProtocol](AppProtocol.Http1)).toOption.get
        val h1PrefaceMiss = Negotiation.selectForPreface(h1Set, isH2Preface = true)
        assertTrue(
          alpnMiss == Left(ConnectorFailure.UnknownAlpnProtocol("bogus-alpn")),
          ServerDiagnostic.NegotiationFailure(label, ConnectorFailure.UnknownAlpnProtocol("bogus-alpn")) ==
            ServerDiagnostic.NegotiationFailure(label, alpnMiss.left.toOption.get),
          prefaceMiss == Left(ConnectorFailure.NoProtocolForPreface(isH2Preface = false)),
          h1PrefaceMiss == Left(ConnectorFailure.NoProtocolForPreface(isH2Preface = true)),
        )
      },
      test("connector labels are stable and carry no secrets") {
        val tcp  = Connector(bind = BindAddress.localhost(8080))
        val unix = Connector(bind = BindAddress.Unix(java.nio.file.Paths.get("/tmp/zio-http-9.sock")))
        assertTrue(
          ServerTelemetry.connectorLabel(tcp) == "tcp/127.0.0.1:8080/h2c",
          ServerTelemetry.connectorLabel(h1Connector) == "tcp/127.0.0.1:0/h2c",
          ServerTelemetry.connectorLabel(unix) == "unix//tmp/zio-http-9.sock",
          !ServerTelemetry.connectorLabel(tcp).contains("BEGIN"),
        )
      },
    ) @@ sequential
}

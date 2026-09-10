package zio.http.h1

import scala.annotation.experimental

import zio._
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.blocks.streams.Stream
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.{Body, Handler, Headers, Method, Request, Response, Route, Routes, Status, handler}
import zio.http.ResultType._
import zio.http.h1.H1RawClientFixture._

/**
 * Todo 7: Loom HTTP/1.1 engine behavior over raw sockets.
 *
 * Exercises the real engine end to end: sequential keep-alive on one
 * connection, streamed (chunked) request and response bodies, HEAD and other
 * bodyless responses, route dispatch (200/404/500), `Connection: close`, and an
 * independent JDK HTTP client. Malformed framing, limits, stalls and lifecycle
 * live in [[H1TransportLimitsSpec]].
 */
@experimental
object H1TransportSpec extends ZIOSpecDefault {

  private val routes: Routes[Any] = Routes(
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
      RoutePattern(Method.GET, "/stream"),
      handler { (_: Request) =>
        responseAsResult(
          Response(
            Status.Ok,
            Headers("content-type" -> "text/plain"),
            Body.fromStream(Stream.fromIterator("abc".getBytes("UTF-8").iterator)),
          ),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/boom"),
      Handler.fromRequest((_: Request) => throw new RuntimeException("boom")),
    ),
    Route(
      RoutePattern(Method.GET, "/hello"),
      handler { (req: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString(req.url.path.encode + "|" + req.url.queryParams.encode)),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/keepalive"),
      handler { (_: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("ka")).setHeader("connection", "keep-alive"),
        )
      },
    ),
  )

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H1TransportSpec")(
      test("serves GET with Content-Length over a raw socket") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp = client.readResponse()
              assertTrue(resp.status == 200, resp.bodyText == "ok", resp.header("content-length").contains("2"))
            } finally client.close()
          }
        }
      },
      test("serves sequential keep-alive requests on one connection") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val first  = client.readResponse()
              client.sendRaw("GET /hello?name=avery HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val second = client.readResponse()
              assertTrue(
                first.status == 200 && first.bodyText == "ok",
                second.status == 200 && second.bodyText == "/hello|name=avery",
                second.header("connection").isEmpty,
              )
            } finally client.close()
          }
        }
      },
      test("answers HEAD with headers only and no body bytes") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("HEAD / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp = client.readResponse(readBody = false)
              assertTrue(
                resp.status == 200,
                resp.body.isEmpty,
                resp.header("content-length").contains("2"),
              )
            } finally client.close()
          }
        }
      },
      test("returns 404 for unknown routes") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET /missing HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp = client.readResponse()
              assertTrue(resp.status == 404)
            } finally client.close()
          }
        }
      },
      test("maps handler defects to 500") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET /boom HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp = client.readResponse()
              assertTrue(resp.status == 500)
            } finally client.close()
          }
        }
      },
      test("echoes chunked request bodies") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw(
                "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n" +
                  "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
              )
              val resp = client.readResponse()
              assertTrue(resp.status == 200, resp.bodyText == "hello world")
            } finally client.close()
          }
        }
      },
      test("streams unknown-length responses as chunked") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              val resp = client.readResponse()
              assertTrue(
                resp.status == 200,
                resp.bodyText == "abc",
                resp.header("transfer-encoding").exists(_.toLowerCase(java.util.Locale.ROOT).contains("chunked")),
              )
            } finally client.close()
          }
        }
      },
      test("serves an independent JDK HTTP client") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client   = java.net.http.HttpClient.newHttpClient()
            val request  =
              java.net.http.HttpRequest
                .newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/hello?name=jdk"))
                .GET()
                .build()
            val response =
              client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
            assertTrue(response.statusCode() == 200, response.body() == "/hello|name=jdk")
          }
        }
      },
      test("closes the connection after Connection: close") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
              val resp = client.readResponse()
              val eof  = client.readByte()
              assertTrue(
                resp.status == 200 && resp.bodyText == "ok",
                resp.header("connection").exists(_.equalsIgnoreCase("close")),
                eof == -1,
              )
            } finally client.close()
          }
        }
      },
      test("lets request close win over a handler keep-alive") {
        withH1Server(routes) { port =>
          ZIO.attemptBlocking {
            val client = new RawH1Client(port)
            try {
              client.sendRaw("GET /keepalive HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
              val resp = client.readResponse()
              val eof  = client.readByte()
              assertTrue(
                resp.status == 200 && resp.bodyText == "ka",
                resp.header("connection").exists(_.equalsIgnoreCase("close")),
                eof == -1,
              )
            } finally client.close()
          }
        }
      },
      test("exposes the H1 ProtocolEngine identity") {
        ZIO.attempt {
          val transport =
            new H1Transport(routes, Context.empty, zio.http.Connector.default, zio.http.DefectHandler.default)
          assertTrue(
            transport.id == zio.http.EngineId("h1"),
            transport.transportKind == zio.http.TransportKind.Tcp,
            transport.supportedProtocols == Set[zio.http.ProtocolId](zio.http.ProtocolId.Http1),
          )
        }
      },
    ) @@ sequential
}

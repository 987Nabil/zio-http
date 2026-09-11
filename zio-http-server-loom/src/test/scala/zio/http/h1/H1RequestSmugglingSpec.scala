package zio.http.h1

import java.io.EOFException
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.{Body, Connector, DefectHandler, Request, Response, Route, Routes, Status, handler}
import zio.http.ResultType._
import zio.http.h1.H1RawClientFixture._

/**
 * Todo 15: H1 request-smuggling and parser-hardening matrix.
 *
 * Locks the strict-codec / H1-engine / cleartext-dispatch security boundary
 * with raw live sockets only. Every smuggling vector (CL.TE, TE.CL, conflicting
 * or duplicated lengths, whitespace/control bytes, obs-fold, chunk-trailer
 * abuse, limits, incomplete bodies) must classify as reject + close: the codec
 * reports a must-close `H1Error`, the engine answers `400` (`413` for oversize
 * bodies) with `Connection: close` and closes, the handler never runs, a
 * smuggled victim request on the same bytes is never served, and a fresh
 * connection stays healthy afterwards.
 *
 * No pipelining, proxy, Upgrade, H3, or implementation changes: tests only.
 */
@experimental
object H1RequestSmugglingSpec extends ZIOSpecDefault {

  private def wire(s: String): Chunk[Byte] =
    Chunk.fromArray(s.getBytes("ISO-8859-1"))

  private def codecRejectsAs[E <: H1Error](raw: String)(implicit
    tag: scala.reflect.ClassTag[E],
  ): Boolean =
    new H1Decoder().feed(wire(raw)) match {
      case Left(error) => tag.runtimeClass.isInstance(error) && error.mustClose
      case Right(_)    => false
    }

  private def countingRoutes(counter: AtomicInteger): Routes[Any] =
    Routes(
      Route(
        RoutePattern.GET,
        handler { (_: Request) =>
          counter.incrementAndGet()
          responseAsResult(Response(status = Status.Ok, body = Body.fromString("victim")))
        },
      ),
      Route(
        RoutePattern(zio.http.Method.POST, "/echo"),
        handler { (req: Request) =>
          counter.incrementAndGet()
          responseAsResult(Response(status = Status.Ok, body = Body.fromString(req.body.asString())))
        },
      ),
    )

  /**
   * Sends one smuggling vector at a live engine and proves the full
   * reject+close contract: expected status with `Connection: close`, the socket
   * closed (clean FIN or RST after unread smuggle bytes), no handler dispatch,
   * no second response on the poisoned socket, and a healthy victim on a fresh
   * connection.
   */
  private def rejectsLive(raw: String, expectedStatus: Int): ZIO[Scope, Throwable, TestResult] = {
    val counter = new AtomicInteger(0)
    withH1Server(countingRoutes(counter)) { port =>
      ZIO.attemptBlocking {
        val client = new RawH1Client(port)
        try {
          client.sendRaw(raw)
          val resp                  = client.readResponse()
          // Unread smuggled bytes may turn the close into a reset instead of
          // a clean FIN; both prove the connection died with the request.
          val closed                =
            try client.readByte() == -1
            catch {
              case _: java.net.SocketException => true
            }
          val retryClosed           =
            try {
              client.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
              client.readResponse()
              "served"
            } catch {
              case _: EOFException                    => "closed"
              case _: java.net.SocketException        => "closed"
              case _: java.net.SocketTimeoutException => "closed"
            }
          val dispatchedBeforeFresh = counter.get()
          val fresh                 = new RawH1Client(port)
          try {
            fresh.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            val victim = fresh.readResponse()
            assertTrue(
              resp.status == expectedStatus,
              resp.header("connection").exists(_.equalsIgnoreCase("close")),
              closed,
              retryClosed == "closed",
              dispatchedBeforeFresh == 0,
              victim.status == 200 && victim.bodyText == "victim",
              counter.get() == 1,
            )
          } finally fresh.close()
        } finally client.close()
      }
    }
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H1RequestSmugglingSpec")(
      suite("strict codec boundary")(
        test("CL.TE classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n",
            ),
          )
        },
        test("TE.CL ordering classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n",
            ),
          )
        },
        test("conflicting Content-Length values classify AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
            ),
          )
        },
        test("duplicated identical Content-Length classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n",
            ),
          )
        },
        test("whitespace before the header colon classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "POST /echo HTTP/1.1\r\nHost : x\r\nContent-Length: 0\r\n\r\n",
            ),
          )
        },
        test("control byte in a header value classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "POST /echo HTTP/1.1\r\nHost: x\u0001\r\nContent-Length: 0\r\n\r\n",
            ),
          )
        },
        test("control byte in a header name classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "POST /echo HTTP/1.1\r\nHo\u007fst: x\r\nContent-Length: 0\r\n\r\n",
            ),
          )
        },
        test("whitespace in the request target classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "GET /vic tim HTTP/1.1\r\nHost: x\r\n\r\n",
            ),
          )
        },
        test("obs-fold classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "POST /echo HTTP/1.1\r\nHost: x\r\n X-Folded: 1\r\nContent-Length: 0\r\n\r\n",
            ),
          )
        },
        test("Trailer without chunked classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nTrailer: X-Sum\r\n\r\n",
            ),
          )
        },
        test("Content-Length inside trailers classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nContent-Length: 5\r\n\r\n",
            ),
          )
        },
        test("Transfer-Encoding inside trailers classifies AmbiguousFraming must-close") {
          assertTrue(
            codecRejectsAs[H1Error.AmbiguousFraming](
              "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nTransfer-Encoding: chunked\r\n\r\n",
            ),
          )
        },
        test("corrupt chunk terminator classifies InvalidMessage must-close") {
          assertTrue(
            codecRejectsAs[H1Error.InvalidMessage](
              "POST /echo HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX\r\n0\r\n\r\n",
            ),
          )
        },
        test("header-count flood classifies TooManyHeaders must-close") {
          val head = new StringBuilder("POST /echo HTTP/1.1\r\nHost: x\r\n")
          var i    = 0
          while (i < 101) {
            head.append("x-flood-").append(i.toString).append(": v\r\n")
            i += 1
          }
          head.append("\r\n")
          assertTrue(codecRejectsAs[H1Error.TooManyHeaders](head.toString))
        },
        test("oversize declared body classifies BodyTooLarge must-close") {
          assertTrue(
            codecRejectsAs[H1Error.BodyTooLarge](
              "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 2000000\r\n\r\n",
            ),
          )
        },
        test("incomplete body blocks without error and completes exactly once") {
          val decoder = new H1Decoder()
          val partial = decoder.feed(wire("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\nAB"))
          val blocked = partial == Right(Nil) && !decoder.poisoned
          val done    = decoder.feed(wire("CDEFGHIJ")) match {
            case Right(List(request)) =>
              request.target == "/echo" &&
              new String(request.body.toArray, "ISO-8859-1") == "ABCDEFGHIJ" &&
              !decoder.poisoned
            case _                    => false
          }
          assertTrue(blocked, done)
        },
      ),
      suite("live reject, close, and no victim contamination")(
        test("CL.TE smuggled victim rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("TE.CL ordering smuggled victim rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("conflicting Content-Length smuggled victim rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("duplicated Content-Length smuggled victim rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("whitespace before the header colon rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost : 127.0.0.1\r\nContent-Length: 0\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("control byte in a header value rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\u0001\r\nContent-Length: 0\r\n\r\n",
            400,
          )
        },
        test("control byte in a header name rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHo\u007fst: 127.0.0.1\r\nContent-Length: 0\r\n\r\n",
            400,
          )
        },
        test("whitespace in the request target rejects 400 with no dispatch") {
          rejectsLive(
            "GET /vic tim HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("obs-fold rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n X-Folded: 1\r\nContent-Length: 0\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("Trailer without chunked rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nTrailer: X-Sum\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("Content-Length inside trailers rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nContent-Length: 5\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("Transfer-Encoding inside trailers rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nTransfer-Encoding: chunked\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("corrupt chunk terminator with smuggled tail rejects 400 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhelloXX\r\n0\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            400,
          )
        },
        test("header-count flood rejects 400 with no dispatch") {
          val head = new StringBuilder("POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n")
          var i    = 0
          while (i < 101) {
            head.append("x-flood-").append(i.toString).append(": v\r\n")
            i += 1
          }
          head.append("\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
          rejectsLive(head.toString, 400)
        },
        test("oversize declared body rejects 413 with no dispatch") {
          rejectsLive(
            "POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2000000\r\n\r\nGET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            413,
          )
        },
        test("incomplete body dispatches nothing and closes with no response") {
          val counter = new AtomicInteger(0)
          withH1Server(countingRoutes(counter)) { port =>
            ZIO.attemptBlocking {
              val dirty                 = new Socket("127.0.0.1", port)
              dirty.setSoTimeout(10000)
              val outcome               =
                try {
                  val out = dirty.getOutputStream
                  out.write("POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\n\r\nAB".getBytes("UTF-8"))
                  out.flush()
                  dirty.shutdownOutput()
                  val in  = dirty.getInputStream
                  val buf = new Array[Byte](1024)
                  val n   = in.read(buf)
                  if (n == -1) "closed" else "bytes:" + n.toString
                } catch {
                  case _: EOFException             => "closed"
                  case _: java.net.SocketException => "closed"
                } finally dirty.close()
              val dispatchedBeforeFresh = counter.get()
              val fresh                 = new RawH1Client(port)
              try {
                fresh.sendRaw("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                val victim = fresh.readResponse()
                assertTrue(
                  outcome == "closed",
                  dispatchedBeforeFresh == 0,
                  victim.status == 200 && victim.bodyText == "victim",
                  counter.get() == 1,
                )
              } finally fresh.close()
            }
          }
        },
      ),
    ) @@ sequential
}

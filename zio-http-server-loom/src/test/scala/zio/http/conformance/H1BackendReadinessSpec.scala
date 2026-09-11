package zio.http.conformance

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.experimental

import zio._
import zio.blocks.chunk.Chunk
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 10: H1 backend readiness.
 *
 * The H1 server engine lands in Todo 7, so [[H1Backend]] must fail fast (never
 * fake green). The H1 client half ([[H1WireClient]]) is real already and is
 * proven here against canned loopback bytes: request bytes hit the wire
 * exactly, and Content-Length, chunked (with trailers), and close-delimited
 * bodies all parse. Todo 7/16 point the same client at the real engine.
 */
@experimental
object H1BackendReadinessSpec extends ZIOSpecDefault {

  private val UsAscii = StandardCharsets.US_ASCII
  private val Utf8    = StandardCharsets.UTF_8

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("H1BackendReadinessSpec")(
      test("engine tags label every diagnostic") {
        assertTrue(
          (ProtocolTag.H1.label, ProtocolTag.H2C.label) == (("H1", "H2C")),
        )
      },
      test("H1 bootstrap fails fast naming Todo 7, never fakes parity") {
        H1Backend
          .withServer(ConformanceCorpus.routes) { _ => ZIO.succeed(assertTrue(true)) }
          .either
          .map { outcome =>
            val message = outcome match {
              case Left(error) => error.getMessage
              case Right(_)    => "<unexpected success>"
            }
            assertTrue(
              ("H1", outcome.isLeft) == (("H1", true)),
              ("H1", message.contains("Todo 7")) == (("H1", true)),
              ("H1", message.contains("H1 engine is not installed")) == (("H1", true)),
            )
          }
      },
      test("H1 defects bootstrap fails fast too") {
        H1Backend
          .withServerAndDefects(ConformanceCorpus.routes, ConformanceCorpus.mappingDefects) { _ =>
            ZIO.succeed(assertTrue(true))
          }
          .either
          .map { outcome =>
            assertTrue(("H1", outcome.isLeft) == (("H1", true)))
          }
      },
      test("H1 wire client: request bytes exact, Content-Length response parses") {
        ZIO.attemptBlocking {
          val canned   = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Echo: hi\r\n\r\nhello".getBytes(UsAscii)
          val recorded = new AtomicReference[String]("")
          val exchange = runCannedExchange(canned, recorded)
          val resp     = exchange.response
          val raw      = recorded.get()
          val threadOk = !exchange.threadAlive
          (resp, raw, threadOk)
        }.map { case (resp, raw, threadOk) =>
          assertTrue(
            ("H1", threadOk) == (("H1", true)),
            ("H1", resp.status) == (("H1", 200)),
            ("H1", resp.bodyText) == (("H1", "hello")),
            ("H1", resp.headerFirst("x-echo")) == (("H1", Some("hi"))),
            ("H1", raw.contains("POST /echo?x=1 HTTP/1.1")) == (("H1", true)),
            ("H1", raw.contains("X-Echo: hi")) == (("H1", true)),
            ("H1", raw.contains("world")) == (("H1", true)),
          )
        }
      },
      test("H1 wire client: chunked body with trailers parses") {
        ZIO.attemptBlocking {
          val canned   =
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trailer: t\r\n\r\n"
              .getBytes(UsAscii)
          val recorded = new AtomicReference[String]("")
          val exchange = runCannedExchange(canned, recorded)
          (exchange.response, !exchange.threadAlive)
        }.map { case (resp, threadOk) =>
          assertTrue(
            ("H1", threadOk) == (("H1", true)),
            ("H1", resp.status) == (("H1", 200)),
            ("H1", resp.bodyText) == (("H1", "hello world")),
          )
        }
      },
      test("H1 wire client: close-delimited body reads to EOF") {
        ZIO.attemptBlocking {
          val canned   = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nbye".getBytes(UsAscii)
          val recorded = new AtomicReference[String]("")
          val exchange = runCannedExchange(canned, recorded)
          (exchange.response, !exchange.threadAlive)
        }.map { case (resp, threadOk) =>
          assertTrue(
            ("H1", threadOk) == (("H1", true)),
            ("H1", resp.status) == (("H1", 200)),
            ("H1", resp.bodyText) == (("H1", "bye")),
          )
        }
      },
    ) @@ sequential

  private final case class CannedExchange(response: ObservedResponse, threadAlive: Boolean)

  /**
   * One deterministic loopback exchange: a server thread reads the full request
   * (headers plus Content-Length body), records it raw, writes canned bytes,
   * and closes. Bounded by socket timeouts and a join deadline; no sleeps
   * anywhere.
   */
  private def runCannedExchange(canned: Array[Byte], recorded: AtomicReference[String]): CannedExchange = {
    val server = new ServerSocket()
    server.bind(new InetSocketAddress("127.0.0.1", 0))
    val worker = new Thread(() => {
      val accepted = server.accept()
      accepted.setSoTimeout(15000)
      try {
        val request = readHttpRequest(accepted.getInputStream)
        recorded.set(new String(request, UsAscii))
        accepted.getOutputStream.write(canned)
        accepted.getOutputStream.flush()
      } catch {
        case _: Exception => ()
      } finally accepted.close()
    })
    worker.setDaemon(true)
    worker.start()
    val socket = new Socket("127.0.0.1", server.getLocalPort)
    socket.setSoTimeout(15000)
    try {
      val client   = new H1WireClient(socket.getInputStream, socket.getOutputStream)
      client.sendRequest(
        "POST",
        "/echo?x=1",
        List("X-Echo" -> "hi", "Content-Type" -> "text/plain"),
        Chunk.fromArray("world".getBytes(Utf8)),
      )
      val response = client.readResponse()
      worker.join(15000)
      CannedExchange(response, worker.isAlive)
    } finally {
      socket.close()
      server.close()
    }
  }

  private def readHttpRequest(in: java.io.InputStream): Array[Byte] = {
    val head          = new ByteArrayOutputStream()
    var done          = false
    while (!done) {
      val b = in.read()
      if (b < 0) throw new java.io.EOFException("EOF inside request head")
      head.write(b)
      done = endsWithDoubleCrlf(head)
    }
    val headerText    = new String(head.toByteArray, UsAscii)
    val contentLength = headerText
      .split("\r\n")
      .map(_.trim)
      .find(_.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:"))
      .map(_.substring("content-length:".length).trim.toInt)
      .getOrElse(0)
    val body          = new Array[Byte](contentLength)
    var offset        = 0
    while (offset < contentLength) {
      val n = in.read(body, offset, contentLength - offset)
      if (n < 0) throw new java.io.EOFException("EOF inside request body")
      offset += n
    }
    val out           = new ByteArrayOutputStream()
    out.write(head.toByteArray)
    out.write(body)
    out.toByteArray
  }

  private def endsWithDoubleCrlf(buf: ByteArrayOutputStream): Boolean = {
    val bytes = buf.toByteArray
    bytes.length >= 4 &&
    bytes(bytes.length - 4) == '\r' &&
    bytes(bytes.length - 3) == '\n' &&
    bytes(bytes.length - 2) == '\r' &&
    bytes(bytes.length - 1) == '\n'
  }
}

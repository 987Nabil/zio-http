package zio.http.h1

import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.experimental
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import zio.blocks.chunk.Chunk
import zio.blocks.context.Context

import zio.http.{
  BindAddress,
  Body,
  BoundAddress,
  BoundConnector,
  BoundConnectorHandle,
  Connector,
  DefectHandler,
  EngineDispatcher,
  EngineId,
  Headers,
  HeadersBuilder,
  InFlightTracker,
  LoomListener,
  Method,
  Path,
  ProtocolEngine,
  ProtocolId,
  QuiescentEngine,
  Request,
  Response,
  Routes,
  TransportKind,
  URL,
  Version,
}

/**
 * Loom HTTP/1.1 engine: strict codec plus the shared application dispatcher.
 *
 * One [[H1Decoder]] per connection feeds fully-decoded [[H1Request]] values to
 * one shared [[EngineDispatcher]]; routes never see bytes. Requests on a
 * connection are served strictly sequentially in wire order (keep-alive); there
 * is no concurrent or out-of-order pipelined execution. Every connection runs
 * on its own Loom virtual thread owned by [[LoomListener]]; blocking reads and
 * writes park that thread, never a ZIO fiber.
 *
 * Request/response confinement: the decoder is per-connection (never shared),
 * the dispatcher holds only the immutable route tree, and [[EngineDispatcher]]
 * opens and closes a per-request [[zio.blocks.scope.Scope]]. The only
 * cross-connection state is the drain flag and the force-close set.
 *
 * Framing policy (authoritative, applied after route handling):
 *   - HEAD and bodyless statuses (1xx/204/304) never carry body bytes; HEAD
 *     still advertises a known `Content-Length`.
 *   - Materialized bodies go out with `Content-Length`; unknown-length streams
 *     go out as `Transfer-Encoding: chunked`, flushed per chunk.
 *   - Handler-set `Content-Length` / `Transfer-Encoding` / `Trailer` headers
 *     are stripped and re-derived, so the wire framing is never ambiguous.
 *   - Malformed framing fails as `400` (oversize bodies as `413`, unknown or
 *     CONNECT methods as `501`) with the connection closed.
 *
 * Deadlines use one-shot virtual-thread sleeps cancelled on completion (the
 * same pattern as `H2ConnectionControl` timers): `requestTimeoutMs` bounds a
 * whole request with a best-effort `408` when still reading, `idleTimeout`
 * bounds inter-request keep-alive waits. No monitor `synchronized` blocks and
 * no sleep polling sit on the read/write hot paths.
 *
 * Out of scope by construction: pipelining, CONNECT tunnels, proxy forwarding
 * headers (passed through untouched), HTTP/1.0, TLS/ALPN dispatch, and
 * aggregate listener lifecycle (see [[ProtocolEngine.drain]] / `close`).
 */
@experimental
final class H1Transport[Ctx](
  routes: Routes[Ctx],
  context: Context[Ctx],
  val connector: Connector,
  defectHandler: DefectHandler,
) extends ProtocolEngine
    with QuiescentEngine {

  def id: EngineId = EngineId("h1")

  def transportKind: TransportKind = TransportKind.Tcp

  def supportedProtocols: Set[ProtocolId] = Set(ProtocolId.Http1)

  /**
   * Block up to `timeout` for in-flight connections to settle (Todo 8): every
   * accepted connection enters [[inFlight]] on accept and exits when its
   * handler returns, so an empty tracker means no live connection remains.
   */
  def awaitQuiescent(timeout: Duration): Boolean = inFlight.awaitEmpty(timeout)

  /**
   * Graceful drain: in-flight requests finish, served with `Connection: close`;
   * new connections close without being read.
   */
  def drain(): Unit = draining.set(true)

  /** Force-close every tracked connection immediately. */
  def close(): Unit = {
    draining.set(true)
    forceCloseTrackers.asScala.foreach { close =>
      try close()
      catch {
        case NonFatal(_) => ()
      }
    }
  }

  def start(): BoundConnectorHandle =
    connector.bind match {
      case BindAddress.Tcp(host, port) =>
        // Cleartext only: TLS/ALPN selection belongs to the dispatch layer of
        // a later todo; this engine never sees a handshake.
        val listener = new LoomListener(host, port, None, conn => serveConnection(conn.input, conn.output))
        val bound    = listener.start()
        BoundConnectorHandle(
          BoundConnector(BoundAddress.Tcp(bound.host, bound.port), connector.protocol),
          bound.close,
          bound.isRunning,
          bound.stopAccepting,
        )
      case BindAddress.Unix(path)      =>
        throw new UnsupportedOperationException("Unix domain sockets are not implemented yet: " + path)
    }

  private val dispatcher: EngineDispatcher[Ctx] = new EngineDispatcher(routes, context, defectHandler)

  private val draining: AtomicBoolean = new AtomicBoolean(false)

  private val forceCloseTrackers = ConcurrentHashMap.newKeySet[() => Unit]()

  /** Live connections owned by this engine (see [[awaitQuiescent]]). */
  private val inFlight = new InFlightTracker()

  private def serveConnection(input: InputStream, output: OutputStream): Unit = {
    val tracker: () => Unit = () => {
      closeQuietly(input)
      closeQuietly(output)
    }
    forceCloseTrackers.add(tracker)
    inFlight.enter()
    try {
      // A connection accepted during drain is new work: close without reading.
      if (!draining.get()) connectionLoop(input, output)
    } catch {
      case NonFatal(_) => ()
    } finally {
      inFlight.exit()
      forceCloseTrackers.remove(tracker)
      closeQuietly(input)
      closeQuietly(output)
    }
  }

  private def connectionLoop(input: InputStream, output: OutputStream): Unit = {
    val decoder = new H1Decoder(H1Limits.Default)
    var alive   = true
    while (alive) {
      val cancelRequest = newRequestGuard(input, output)
      try {
        readOneRequest(decoder, input) match {
          case H1Transport.InboundClosed             => alive = false
          case H1Transport.InboundMalformed(error)   =>
            writeError(output, H1Transport.statusFor(error), H1Transport.reasonFor(error))
            alive = false
          case H1Transport.InboundRequests(requests) =>
            var index = 0
            while (index < requests.length && alive && !cancelRequest.expired.get()) {
              alive = handleAndRespond(requests(index), output)
              index += 1
            }
            if (cancelRequest.expired.get()) alive = false
        }
      } finally cancelRequest.cancel()
    }
  }

  /**
   * Arms the whole-request deadline for one keep-alive turn: `requestTimeoutMs`
   * from the first read until the turn ends. On expiry the streams close, which
   * unblocks the turn's read or write and ends the connection; there is no
   * concurrent error write, so a racing response can never interleave with it.
   * Non-positive disables.
   */
  private def newRequestGuard(input: InputStream, output: OutputStream): H1Transport.RequestGuard = {
    val expired    = new AtomicBoolean(false)
    val cancel     = H1Transport.armDeadline(
      connector.requestTimeoutMs,
      () => {
        expired.set(true)
        closeQuietly(input)
        closeQuietly(output)
      },
    )
    // The inter-request idle wait shares the turn: `idleTimeout` closes a
    // connection nobody speaks on anymore. Whichever guard fires first wins;
    // both are cancelled when the turn ends.
    val cancelIdle =
      H1Transport.armDeadline(connector.idleTimeout.toMillis, () => closeQuietly(input))
    new H1Transport.RequestGuard(
      expired,
      () => {
        cancel()
        cancelIdle()
      },
    )
  }

  private def readOneRequest(decoder: H1Decoder, input: InputStream): H1Transport.Inbound = {
    val buf                          = new Array[Byte](8192)
    var outcome: H1Transport.Inbound = null
    while (outcome == null) {
      val n =
        try input.read(buf, 0, buf.length)
        catch {
          case NonFatal(_) => -2
        }
      if (n < 0) outcome = H1Transport.InboundClosed
      else if (n > 0) {
        decoder.feed(Chunk.fromArray(java.util.Arrays.copyOf(buf, n))) match {
          case Left(error)     => outcome = H1Transport.InboundMalformed(error)
          case Right(Nil)      => ()
          case Right(requests) => outcome = H1Transport.InboundRequests(requests)
        }
      }
    }
    outcome
  }

  /**
   * Decodes one request, dispatches it through the shared dispatcher, and
   * writes the response. Returns false when the connection must close
   * afterwards. Conversion and dispatch failures already carry their terminal
   * response; only the connection state is returned.
   */
  private def handleAndRespond(h1request: H1Request, output: OutputStream): Boolean =
    try {
      val methodName = h1request.method
      if (methodName == "CONNECT") {
        writeError(output, 501, "Not Implemented")
        false
      } else {
        Method.fromString(methodName) match {
          case None         =>
            writeError(output, 501, "Not Implemented")
            false
          case Some(method) =>
            buildUrl(method, h1request.target) match {
              case None      =>
                writeError(output, 400, "Bad Request")
                false
              case Some(url) =>
                if (h1request.body.length.toLong > connector.maxRequestBodySize) {
                  writeError(output, 413, "Payload Too Large")
                  false
                } else {
                  val request  = Request(
                    method,
                    url,
                    buildHeaders(h1request.headers),
                    Body.fromChunk(h1request.body),
                    Version.`HTTP/1.1`,
                  )
                  val response =
                    try dispatcher.dispatch(request)
                    catch {
                      case NonFatal(_) => Response.internalServerError
                    }
                  writeResponse(method, h1request, response, output)
                }
            }
        }
      }
    } catch {
      case NonFatal(_) =>
        writeError(output, 500, "Internal Server Error")
        false
    }

  private def buildUrl(method: Method, target: String): Option[URL] =
    if (target == "*") {
      if (method == Method.OPTIONS) Some(URL.fromPath(Path.root))
      else None
    } else {
      URL.parse(target) match {
        case Right(url) => Some(url)
        case Left(_)    => None
      }
    }

  private def buildHeaders(headers: H1Headers): Headers = {
    val pairs   = H1ModelConversion.toFieldPairs(headers)
    val builder = HeadersBuilder.make(pairs.length)
    var index   = 0
    while (index < pairs.length) {
      builder.add(pairs(index)._1, pairs(index)._2)
      index += 1
    }
    builder.build()
  }

  private def writeResponse(
    method: Method,
    h1request: H1Request,
    response: Response,
    output: OutputStream,
  ): Boolean = {
    val requestClose =
      H1ModelConversion.connectionPreference(h1request.headers) == H1ConnectionPreference.Close
    val mustClose    = requestClose || draining.get()
    val status       = response.status.code
    val isHead       = method == Method.HEAD
    val bodyless     = isHead || status == 204 || status == 304 || (status >= 100 && status <= 199)
    if (bodyless) {
      val base    = H1Transport.stripFraming(response.headers.toList)
      val withLen =
        if (isHead) {
          response.body.length match {
            case Some(length) => base :+ ("content-length" -> length.toString)
            case None         => base
          }
        } else base
      H1Transport.toH1Headers(withLen) match {
        case None          =>
          writeError(output, 500, "Internal Server Error")
          false
        case Some(headers) =>
          writeEncoded(
            output,
            H1Response(
              status,
              response.status.text,
              headers,
              H1Transport.emptyChunk,
              H1BodyFraming.Empty,
              H1Headers.Empty,
            ),
            mustClose,
          )
      }
    } else {
      response.body.toStream.knownChunk match {
        case Some(chunk) =>
          H1Transport.toH1Headers(H1Transport.stripFraming(response.headers.toList)) match {
            case None          =>
              writeError(output, 500, "Internal Server Error")
              false
            case Some(headers) =>
              val framing   = H1BodyFraming.Fixed(chunk.length.toLong)
              val h1message = H1Response(status, response.status.text, headers, chunk, framing, H1Headers.Empty)
              writeEncoded(output, h1message, mustClose)
          }
        case None        =>
          if (response.body.length.contains(0L)) {
            H1Transport.toH1Headers(H1Transport.stripFraming(response.headers.toList)) match {
              case None          =>
                writeError(output, 500, "Internal Server Error")
                false
              case Some(headers) =>
                val framing   = H1BodyFraming.Fixed(0L)
                val h1message =
                  H1Response(status, response.status.text, headers, H1Transport.emptyChunk, framing, H1Headers.Empty)
                writeEncoded(output, h1message, mustClose)
            }
          } else writeChunked(response, output, mustClose)
      }
    }
  }

  /**
   * Materialized-path writer: the strict codec owns the exact wire shape, so a
   * `Left` here means the handler produced an unserializable response (unknown
   * status, bad header) and the connection must not stay open.
   */
  private def writeEncoded(output: OutputStream, response: H1Response, mustClose: Boolean): Boolean = {
    val preference =
      H1ModelConversion.connectionPreference(response.headers) == H1ConnectionPreference.Close
    // Authoritative close: a handler `keep-alive` must not survive drain or a
    // `Connection: close` request — strip any Connection header, then pin close.
    val headers    =
      if (mustClose)
        H1Headers(
          response.headers.fields.filterNot(_.name.equalsIgnoreCase("Connection")) :+
            H1Header("Connection", "close"),
        )
      else response.headers
    H1Encoder.encodeResponse(response.copy(headers = headers)) match {
      case Left(_)      =>
        writeError(output, 500, "Internal Server Error")
        false
      case Right(bytes) =>
        try {
          output.write(bytes.toArray)
          output.flush()
          !(mustClose || preference)
        } catch {
          case NonFatal(_) => false
        }
    }
  }

  /**
   * Unknown-length bodies stream as `Transfer-Encoding: chunked`, flushed per
   * chunk so latency-sensitive bodies keep their pacing. A mid-stream failure
   * aborts iteration at once (no buffering, no spin) and closes the connection:
   * the framing is already on the wire, so no error response can follow it.
   */
  private def writeChunked(response: Response, output: OutputStream, mustClose: Boolean): Boolean = {
    H1Transport.toH1Headers(H1Transport.stripFraming(response.headers.toList)) match {
      case None          =>
        writeError(output, 500, "Internal Server Error")
        false
      case Some(headers) =>
        val preference   =
          H1ModelConversion.connectionPreference(headers) == H1ConnectionPreference.Close
        // Same authoritative-close rule as the encoded path: never emit two
        // Connection headers, never let a handler keep-alive survive close.
        val finalHeaders =
          if (mustClose || preference)
            H1Headers(
              headers.fields.filterNot(_.name.equalsIgnoreCase("Connection")) :+
                H1Header("Connection", "close"),
            )
          else headers
        val head         =
          H1Transport.responseHead(response.status.code, response.status.text, finalHeaders)
        try {
          output.write(head)
          output.flush()
          response.body.toStream.chunked(16384).runForeach { chunk =>
            if (!chunk.isEmpty) H1Transport.writeChunk(output, chunk)
          } match {
            case Right(())        =>
              output.write(H1Transport.terminalChunk)
              output.flush()
              !(mustClose || preference)
            case Left(impossible) => throw impossible
          }
        } catch {
          case _: H1Transport.WriteAborted => false
          case NonFatal(_)                 => false
        }
    }
  }

  private def writeError(output: OutputStream, code: Int, reason: String): Unit = {
    val body = reason.getBytes(StandardCharsets.US_ASCII)
    val head =
      "HTTP/1.1 " + code.toString + " " + reason +
        "\r\nContent-Length: " + body.length.toString +
        "\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"
    try {
      output.write(head.getBytes(StandardCharsets.US_ASCII))
      output.write(body)
      output.flush()
    } catch {
      case NonFatal(_) => ()
    }
  }

  private def closeQuietly(resource: AutoCloseable): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case NonFatal(_) => ()
      }
    }
}

@experimental
object H1Transport {

  private[h1] sealed trait Inbound

  private[h1] case object InboundClosed extends Inbound

  private[h1] final case class InboundMalformed(error: H1Error) extends Inbound

  private[h1] final case class InboundRequests(requests: List[H1Request]) extends Inbound

  /**
   * Per-turn deadline state: `expired` trips when the guard fires, `cancel`
   * disarms both the request and idle guards.
   */
  private[h1] final class RequestGuard(val expired: AtomicBoolean, val cancel: () => Unit)

  /**
   * Mid-stream abort marker: the response started (framing on the wire) and
   * then failed, so the connection must close with no error response. Static on
   * the companion so the type test compiles on Scala 2.13, and stackless:
   * aborts are routine control flow, not defects.
   */
  private final class WriteAborted extends RuntimeException("H1 chunked response aborted") {
    override def fillInStackTrace(): Throwable = this
  }

  private val emptyChunk: Chunk[Byte] = Chunk.fromArray(new Array[Byte](0))

  private val terminalChunk: Array[Byte] = "0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)

  private[h1] def statusFor(error: H1Error): Int =
    error match {
      case _: H1Error.BodyTooLarge => 413
      case _                       => 400
    }

  private[h1] def reasonFor(error: H1Error): String =
    error match {
      case _: H1Error.BodyTooLarge => "Payload Too Large"
      case _                       => "Bad Request"
    }

  /**
   * One-shot virtual-thread deadline: sleeps once, fires once, dies on cancel.
   * Never a poll loop; healthy turns pay one interrupt. Non-positive disables.
   */
  private[h1] def armDeadline(timeoutMs: Long, onExpire: () => Unit): () => Unit =
    if (timeoutMs <= 0L) () => ()
    else {
      val live   = new AtomicBoolean(true)
      val thread = Thread
        .ofVirtual()
        .name("zio-http-h1-deadline")
        .start(new Runnable {
          def run(): Unit =
            try {
              Thread.sleep(timeoutMs)
              if (live.compareAndSet(true, false)) onExpire()
            } catch {
              case _: InterruptedException => ()
            }
        })
      () => if (live.compareAndSet(true, false)) thread.interrupt()
    }

  /**
   * Strips engine-owned framing headers so the wire framing is derived exactly
   * once, downstream. `Trailer` goes too: request trailers are never forwarded
   * and responses never emit them.
   */
  private[h1] def stripFraming(pairs: List[(String, String)]): List[(String, String)] =
    pairs.filterNot { pair =>
      pair._1.equalsIgnoreCase("content-length") ||
      pair._1.equalsIgnoreCase("transfer-encoding") ||
      pair._1.equalsIgnoreCase("trailer")
    }

  private[h1] def toH1Headers(pairs: List[(String, String)]): Option[H1Headers] =
    H1ModelConversion.fromFieldPairs(pairs) match {
      case Right(headers) => Some(headers)
      case Left(_)        => None
    }

  private[h1] def responseHead(
    status: Int,
    reason: String,
    headers: H1Headers,
  ): Array[Byte] = {
    val head = new StringBuilder("HTTP/1.1 " + status.toString + " " + reason + "\r\n")
    headers.fields.foreach { header =>
      head.append(header.name).append(": ").append(header.value).append("\r\n")
    }
    head.append("Transfer-Encoding: chunked\r\n\r\n")
    head.toString.getBytes(StandardCharsets.US_ASCII)
  }

  private[h1] def writeChunk(output: OutputStream, chunk: Chunk[Byte]): Unit =
    try {
      val head = Integer.toHexString(chunk.length).toUpperCase(java.util.Locale.ROOT) + "\r\n"
      output.write(head.getBytes(StandardCharsets.US_ASCII))
      output.write(chunk.toArray)
      output.write('\r')
      output.write('\n')
      output.flush()
    } catch {
      case NonFatal(_) => throw new WriteAborted
    }
}

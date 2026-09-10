package zio.http.conformance

import java.io.EOFException
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.annotation.experimental
import scala.collection.mutable.ListBuffer

import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.test.TestResult

import zio.http.h2.FrameCodec
import zio.http.h2.H2Error
import zio.http.h2.H2Frame
import zio.http.h2.H2Frame._
import zio.http.h2.H2RawClientFixture
import zio.http.h2.H2Transport
import zio.http.h2.hpack.{HeaderField, HpackDecoder, HpackEncoder}
import zio.http.{BindAddress, BoundAddress, Connector, DefectHandler, Routes, ServerHandle}

/**
 * Todo 10: protocol-neutral conformance backend abstraction.
 *
 * The corpus ([[ConformanceHarness.suiteFor]]) runs unchanged per engine: a
 * backend owns server bootstrap plus a wire client that reports
 * [[ObservedResponse]] values. No H2 frames, HPACK tables, or H1 bytes leak
 * past the backend boundary, so corpus assertions stay protocol-neutral and
 * every failure diagnostic carries the engine tag.
 *
 * Live today: [[H2CBackend]]. [[H1Backend]] fails fast until Todo 7 installs
 * the Loom H1 engine; its client half ([[H1WireClient]]) is already real and
 * covered by [[H1BackendReadinessSpec]] against canned bytes.
 */
@experimental
sealed trait ProtocolTag {
  def label: String
}

@experimental
object ProtocolTag {

  case object H1 extends ProtocolTag {
    val label: String = "H1"
  }

  case object H2C extends ProtocolTag {
    val label: String = "H2C"
  }
}

/**
 * Engine-neutral response observed by the corpus. Header names are lowercase.
 */
final case class ObservedResponse(
  status: Int,
  headers: Map[String, List[String]],
  body: Chunk[Byte],
) {

  def bodyText: String =
    new String(body.toArray, StandardCharsets.UTF_8)

  def headerFirst(name: String): Option[String] =
    headers.get(name.toLowerCase(Locale.ROOT)).flatMap(_.headOption)
}

/** Engine-neutral partial read for the cancellation probe. */
final case class AbortedRead(
  status: Int,
  headers: Map[String, List[String]],
  firstBytes: Chunk[Byte],
)

/** Engine-neutral client: full exchanges plus an aborting partial read. */
@experimental
trait ConformanceClient {

  def request(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse

  def getThenAbort(path: String): AbortedRead

  def close(): Unit
}

/** Engine-neutral server bootstrap for one corpus run. */
@experimental
trait ConformanceBackend {

  def tag: ProtocolTag

  def withServer[R](
    routes: Routes[Any],
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult]

  def withServerAndDefects[R](
    routes: Routes[Any],
    defects: DefectHandler,
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult]
}

/** Live H2C backend: real H2Transport plus a dedicated H2 wire client. */
@experimental
object H2CBackend extends ConformanceBackend {

  val tag: ProtocolTag = ProtocolTag.H2C

  def withServer[R](
    routes: Routes[Any],
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
    H2RawClientFixture.withRawServer(routes) { port =>
      use(new H2ConformanceClient(port))
    }

  def withServerAndDefects[R](
    routes: Routes[Any],
    defects: DefectHandler,
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(
        ZIO.attempt(
          ServerHandle.live(
            List(
              new H2Transport(routes, Context.empty, Connector(bind = BindAddress.localhost(0)), defects).start(),
            ),
          ),
        ),
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, value) => value
          case other                      => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use(new H2ConformanceClient(port))
      }
}

/**
 * H1 backend slot. The server half does not exist yet (Todo 7 owns the Loom H1
 * engine), so bootstrap fails fast with an explicit [[H1EngineMissing]] rather
 * than faking protocol parity. Todo 7/16 replace the failure with a real
 * cleartext H1 bootstrap; the corpus needs no changes.
 */
final case class H1EngineMissing()
    extends Exception(
      "H1 engine is not installed yet: Todo 7 owns the Loom H1 engine. " +
        "H1 conformance legs wire into ConformanceHarness.suiteFor(H1Backend) once serve() exists.",
    )

@experimental
object H1Backend extends ConformanceBackend {

  val tag: ProtocolTag = ProtocolTag.H1

  def withServer[R](
    routes: Routes[Any],
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
    ZIO.fail(H1EngineMissing())

  def withServerAndDefects[R](
    routes: Routes[Any],
    defects: DefectHandler,
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
    ZIO.fail(H1EngineMissing())
}

/**
 * H2 wire client for the conformance corpus: one connection per exchange, raw
 * frames, window top-up on every DATA frame like a real receiver (a 96KB
 * streamed download would otherwise stall the server FlowController forever).
 */
@experimental
final class H2ConformanceClient(port: Int) extends ConformanceClient {

  def request(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse = {
    val connection = new H2ConformanceConnection(port)
    try connection.roundTrip(method, path, headers, body)
    finally connection.close()
  }

  def getThenAbort(path: String): AbortedRead = {
    val connection = new H2ConformanceConnection(port)
    try connection.getThenAbort(path)
    finally connection.close()
  }

  def close(): Unit = ()
}

@experimental
private final class H2ConformanceConnection(port: Int) extends AutoCloseable {

  private val UsAscii         = StandardCharsets.US_ASCII
  private val PrefaceBytes    = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(UsAscii)
  private val MaxFramePayload = 16384
  private val SocketTimeoutMs = 20000

  private val socket = new Socket("127.0.0.1", port)
  socket.setSoTimeout(SocketTimeoutMs)
  private val out    = socket.getOutputStream
  private val rawIn  = socket.getInputStream
  private var buf    = Chunk.empty[Byte]

  private val encoder = new HpackEncoder()
  private val decoder = new HpackDecoder()

  handshake()

  def roundTrip(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse = {
    sendRequest(method, path, headers, body, streamId = 1)
    awaitFull(streamId = 1)
  }

  def getThenAbort(path: String): AbortedRead = {
    sendRequest("GET", path, Nil, Chunk.empty, streamId = 1)
    awaitPartial(streamId = 1)
  }

  override def close(): Unit = socket.close()

  private def sendRequest(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
    streamId: Int,
  ): Unit = {
    val pseudo = List(
      HeaderField(":method", method),
      HeaderField(":path", path),
      HeaderField(":scheme", "http"),
      HeaderField(":authority", "127.0.0.1:" + port),
    )
    val extra  = headers.map { case (name, value) => HeaderField(name.toLowerCase(Locale.ROOT), value) }
    sendFrame(
      Headers(
        streamId = streamId,
        headerBlock = encoder.encode(pseudo ++ extra),
        endStream = body.isEmpty,
        endHeaders = true,
      ),
    )
    if (body.nonEmpty) {
      val bytes  = body.toArray
      var offset = 0
      while (offset < bytes.length) {
        val end   = Math.min(offset + MaxFramePayload, bytes.length)
        val slice = Chunk.fromArray(java.util.Arrays.copyOfRange(bytes, offset, end))
        sendFrame(Data(streamId, slice, endStream = end == bytes.length))
        offset = end
      }
    }
  }

  private def awaitFull(streamId: Int): ObservedResponse = {
    val hdrs   = ListBuffer.empty[HeaderField]
    var body   = Chunk.empty[Byte]
    var done   = false
    var status = -1
    while (!done) {
      readFrame() match {
        case Settings(false, _)                                   => sendFrame(Settings(ack = true, Nil))
        case Settings(true, _)                                    => ()
        case _: WindowUpdate                                      => ()
        case Ping(false, data)                                    => sendFrame(Ping(ack = true, data))
        case Headers(sid, block, end, _, _, _) if sid == streamId =>
          val fields = decode(block)
          hdrs ++= fields
          status = fields.find(_.name == ":status").map(_.value.toInt).getOrElse(status)
          done = end
        case Headers(_, block, _, _, _, _)                        =>
          decode(block)
        case Data(sid, data, end, _) if sid == streamId           =>
          body = body ++ data
          topUp(sid, data.length)
          done = end
        case Data(sid, data, _, _)                                =>
          topUp(sid, data.length)
        case GoAway(_, code, debug)                               =>
          throw new AssertionError("Unexpected GOAWAY: " + code + " " + new String(debug.toArray, UsAscii))
        case _                                                    => ()
      }
    }
    if (status < 0) throw new AssertionError("Response carried no :status header")
    ObservedResponse(status, headerMap(hdrs.toList), body)
  }

  private def awaitPartial(streamId: Int): AbortedRead = {
    val hdrs      = ListBuffer.empty[HeaderField]
    var status    = -1
    var first     = Chunk.empty[Byte]
    var haveFirst = false
    while (!haveFirst) {
      readFrame() match {
        case Settings(false, _)                                   => sendFrame(Settings(ack = true, Nil))
        case Settings(true, _)                                    => ()
        case _: WindowUpdate                                      => ()
        case Ping(false, data)                                    => sendFrame(Ping(ack = true, data))
        case Headers(sid, block, end, _, _, _) if sid == streamId =>
          val fields = decode(block)
          hdrs ++= fields
          status = fields.find(_.name == ":status").map(_.value.toInt).getOrElse(status)
          if (end) haveFirst = true
        case Headers(_, block, _, _, _, _)                        =>
          decode(block)
        case Data(sid, data, _, _) if sid == streamId             =>
          first = first ++ data
          topUp(sid, data.length)
          haveFirst = true
        case Data(sid, data, _, _)                                =>
          topUp(sid, data.length)
        case GoAway(_, code, debug)                               =>
          throw new AssertionError("Unexpected GOAWAY: " + code + " " + new String(debug.toArray, UsAscii))
        case _                                                    => ()
      }
    }
    if (status < 0) throw new AssertionError("Partial response carried no :status header")
    AbortedRead(status, headerMap(hdrs.toList), first)
  }

  private def decode(block: Chunk[Byte]): List[HeaderField] =
    decoder.decode(block) match {
      case Right(fields) => fields
      case Left(error)   => throw new AssertionError("HPACK decode failed: " + error)
    }

  private def headerMap(fields: List[HeaderField]): Map[String, List[String]] =
    fields.groupMap(_.name.toLowerCase(Locale.ROOT))(_.value)

  /**
   * Replenish sender windows like a real receiver; keeps streamed sends
   * flowing.
   */
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
        val n   = rawIn.read(tmp)
        if (n < 0) throw new EOFException("Connection closed mid-frame")
        buf = buf ++ Chunk.fromArray(java.util.Arrays.copyOf(tmp, n))
        readFrame()
      case Left(error)                    =>
        throw new AssertionError("Frame decode failed: " + error)
    }

  private def handshake(): Unit = {
    out.write(PrefaceBytes)
    out.write(FrameCodec.encode(Settings(ack = false, Nil)).toArray)
    out.flush()
    readFrame() match {
      case Settings(false, _) => sendFrame(Settings(ack = true, Nil))
      case other              => throw new AssertionError("Expected server SETTINGS but received: " + other)
    }
    readFrame()
  }
}

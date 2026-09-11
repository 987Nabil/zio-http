package zio.http.conformance

import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.annotation.experimental
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import zio.Scope
import zio.ZIO
import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.test.TestResult

import zio.http.h2.FrameCodec
import zio.http.h2.H2Error
import zio.http.h2.H2Frame
import zio.http.h2.H2Frame._
import zio.http.h2.hpack.HeaderField
import zio.http.h2.hpack.HpackDecoder
import zio.http.h2.hpack.HpackEncoder
import zio.http.h1.H1Transport
import zio.http.h2.H2Engine
import zio.http.TlsAlpnFixtures
import zio.http.{
  BindAddress,
  BoundAddress,
  Connector,
  DefectHandler,
  H1H2CleartextEngine,
  H1H2TlsEngine,
  LoomServer,
  NegotiationPolicy,
  Protocol,
  ProtocolEngine,
  Routes,
  Server,
}

/**
 * Todo 16: one unchanged route corpus served through every production
 * transport.
 *
 * Each backend below serves [[ConformanceCorpus.routes]] via [[LoomServer]]
 * with the engine the production stack uses for that transport — never a test
 * double — and talks to it with a true client surface (raw sockets,
 * [[H1WireClient]] over cleartext or TLS streams, the JDK HTTP client). The
 * corpus ([[ConformanceHarness.suiteFor]]) runs unchanged per backend, so every
 * failure diagnostic carries the engine tag and no per-protocol branch can hide
 * divergence.
 *
 * Backends (7 legs):
 *   - h1 / h2c: single-protocol engines ([[H1Transport]] / [[H2Engine]]);
 *   - tlsH2: TLS H2 through [[H2Engine]] with a negotiated `h2` ALPN id;
 *   - sharedTlsH1 / sharedTlsH2: one [[H1H2TlsEngine]] port observed once per
 *     negotiated protocol;
 *   - sharedCleartextH1 / sharedCleartextH2c: one [[H1H2CleartextEngine]] port
 *     observed once per sniffed protocol.
 */
@experimental
object ProtocolParityBackends {

  val h1: ConformanceBackend =
    single(
      ProtocolTag.H1,
      Connector(bind = BindAddress.localhost(0)),
      (routes, defects, connector) => new H1Transport(routes, Context.empty, connector, defects),
      port => new ParityCleartextH1Client(port),
    )

  val h2c: ConformanceBackend =
    single(
      ProtocolTag.H2C,
      Connector(bind = BindAddress.localhost(0)),
      (routes, defects, connector) => new H2Engine(routes, Context.empty, connector, defects),
      port => new H2ConformanceClient(port),
    )

  val tlsH2: ConformanceBackend =
    single(
      ProtocolTag.H2C,
      Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig())),
      (routes, defects, connector) => new H2Engine(routes, Context.empty, connector, defects),
      port => new ParityJdkH2Client(port),
    )

  val sharedTlsH1: ConformanceBackend =
    single(
      ProtocolTag.H1,
      Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig())),
      (routes, defects, connector) => new H1H2TlsEngine(routes, Context.empty, connector, defects),
      port => new ParityTlsH1Client(port),
    )

  val sharedTlsH2: ConformanceBackend =
    single(
      ProtocolTag.H2C,
      Connector(bind = BindAddress.localhost(0), protocol = Protocol.H2(TlsAlpnFixtures.tlsConfig())),
      (routes, defects, connector) => new H1H2TlsEngine(routes, Context.empty, connector, defects),
      port => new ParityJdkH2Client(port),
    )

  val sharedCleartextH1: ConformanceBackend =
    single(
      ProtocolTag.H1,
      Connector(
        bind = BindAddress.localhost(0),
        protocol = Protocol.H2C(),
        negotiation = NegotiationPolicy.CleartextPreface,
      ),
      (routes, defects, connector) => new H1H2CleartextEngine(routes, Context.empty, connector, defects),
      port => new ParityCleartextH1Client(port),
    )

  val sharedCleartextH2c: ConformanceBackend =
    single(
      ProtocolTag.H2C,
      Connector(
        bind = BindAddress.localhost(0),
        protocol = Protocol.H2C(),
        negotiation = NegotiationPolicy.CleartextPreface,
      ),
      (routes, defects, connector) => new H1H2CleartextEngine(routes, Context.empty, connector, defects),
      port => new H2ConformanceClient(port),
    )

  private def single(
    engineTag: ProtocolTag,
    connector: Connector,
    makeEngine: (Routes[Any], DefectHandler, Connector) => ProtocolEngine,
    makeClient: Int => ConformanceClient,
  ): ConformanceBackend =
    new ConformanceBackend {
      val tag: ProtocolTag = engineTag

      def withServer[R](
        routes: Routes[Any],
      )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
        serveWith(routes, DefectHandler.default, connector, makeEngine, makeClient)(use)

      def withServerAndDefects[R](
        routes: Routes[Any],
        defects: DefectHandler,
      )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
        serveWith(routes, defects, connector, makeEngine, makeClient)(use)
    }

  private def serveWith[R](
    routes: Routes[Any],
    defects: DefectHandler,
    connector: Connector,
    makeEngine: (Routes[Any], DefectHandler, Connector) => ProtocolEngine,
    makeClient: Int => ConformanceClient,
  )(use: ConformanceClient => ZIO[R, Throwable, TestResult]): ZIO[R with Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(
        ZIO.attempt {
          val engine = makeEngine(routes, defects, connector)
          val server = LoomServer(connector).withEngine(engine)
          Server.serve(routes, Context.empty.add(server))
        },
      )(handle => ZIO.succeed(handle.shutdownAndWait()))
      .flatMap { handle =>
        val port = handle.bindings.head.address match {
          case BoundAddress.Tcp(_, value) => value
          case other                      => throw new AssertionError("Expected TCP binding but found: " + other)
        }
        use(makeClient(port))
      }
}

/**
 * Todo 16: cleartext H1 conformance client — one `Connection: close` exchange
 * per request plus an aborting partial read for the cancellation probe.
 */
@experimental
final class ParityCleartextH1Client(port: Int) extends ConformanceClient {

  private val SocketTimeoutMs = 10000

  def request(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse = {
    val socket = new Socket("127.0.0.1", port)
    socket.setSoTimeout(SocketTimeoutMs)
    try {
      val wire = new H1WireClient(socket.getInputStream, socket.getOutputStream)
      wire.sendRequest(method, path, headers, body)
      // HEAD responses advertise Content-Length with zero body bytes on the
      // wire (RFC 9110 section 9.3.2): parse headers only, never read a body.
      if (method == "HEAD") ParityH1IO.readHeadResponse(socket.getInputStream)
      else wire.readResponse()
    } finally socket.close()
  }

  def getThenAbort(path: String): AbortedRead = {
    val socket = new Socket("127.0.0.1", port)
    socket.setSoTimeout(SocketTimeoutMs)
    try ParityH1IO.partialGet(socket.getInputStream, socket.getOutputStream, path)
    finally socket.close()
  }

  def close(): Unit = ()
}

/**
 * Todo 16: TLS H1 conformance client — the same [[H1WireClient]] framing
 * running over an ALPN-negotiated `http/1.1` TLS stream. A JDK client pinned to
 * HTTP/1.1 sends no ALPN and is closed by dispatch policy, so every exchange
 * here offers `http/1.1` exactly.
 */
@experimental
final class ParityTlsH1Client(port: Int) extends ConformanceClient {

  private val SocketTimeoutMs = 10000

  def request(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse = {
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(SocketTimeoutMs)
    val sslSocket = TlsAlpnFixtures
      .trustAllContext()
      .getSocketFactory
      .createSocket(rawSocket, "127.0.0.1", port, true)
      .asInstanceOf[javax.net.ssl.SSLSocket]
    try {
      val params = sslSocket.getSSLParameters
      params.setApplicationProtocols(Array("http/1.1"))
      sslSocket.setSSLParameters(params)
      sslSocket.setUseClientMode(true)
      sslSocket.startHandshake()
      if (sslSocket.getApplicationProtocol != "http/1.1")
        throw new AssertionError("Expected http/1.1 ALPN but negotiated: " + sslSocket.getApplicationProtocol)
      val wire   = new H1WireClient(sslSocket.getInputStream, sslSocket.getOutputStream)
      wire.sendRequest(method, path, headers, body)
      if (method == "HEAD") ParityH1IO.readHeadResponse(sslSocket.getInputStream)
      else wire.readResponse()
    } finally {
      try sslSocket.close()
      catch { case _: Throwable => () }
    }
  }

  def getThenAbort(path: String): AbortedRead = {
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(SocketTimeoutMs)
    val sslSocket = TlsAlpnFixtures
      .trustAllContext()
      .getSocketFactory
      .createSocket(rawSocket, "127.0.0.1", port, true)
      .asInstanceOf[javax.net.ssl.SSLSocket]
    try {
      val params = sslSocket.getSSLParameters
      params.setApplicationProtocols(Array("http/1.1"))
      sslSocket.setSSLParameters(params)
      sslSocket.setUseClientMode(true)
      sslSocket.startHandshake()
      ParityH1IO.partialGet(sslSocket.getInputStream, sslSocket.getOutputStream, path)
    } finally {
      try sslSocket.close()
      catch { case _: Throwable => () }
    }
  }

  def close(): Unit = ()
}

/**
 * Todo 16: TLS H2 conformance client — the independent JDK HTTP/2
 * implementation for full exchanges, plus a raw TLS+H2 partial read for the
 * cancellation probe (the JDK client has no abort-mid-body surface).
 */
@experimental
final class ParityJdkH2Client(port: Int) extends ConformanceClient {

  private val client =
    TlsAlpnFixtures.newJdkClient(java.net.http.HttpClient.Version.HTTP_2)

  def request(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): ObservedResponse = {
    val builder  = HttpRequest
      .newBuilder(URI.create("https://127.0.0.1:" + port + path))
      .timeout(java.time.Duration.ofSeconds(10))
    headers.foreach { case (name, value) => builder.header(name, value) }
    if (body.isEmpty) builder.method(method, HttpRequest.BodyPublishers.noBody())
    else builder.method(method, HttpRequest.BodyPublishers.ofByteArray(body.toArray))
    val response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
    val mapped   = response
      .headers()
      .map()
      .asScala
      .toList
      .groupMap(_._1.toLowerCase(Locale.ROOT))(_._2.asScala.toList)
      .view
      .mapValues(_.flatten)
      .toMap
    ObservedResponse(response.statusCode(), mapped, Chunk.fromArray(response.body()))
  }

  def getThenAbort(path: String): AbortedRead =
    ParityTlsH2IO.partialGet(port, path)

  def close(): Unit = ()
}

/**
 * Todo 16: shared H1 byte-level helper — one GET with `Connection: close`, then
 * a bounded partial body read. `/cancel/big` is a materialized Content-Length
 * body on the H1 engine, so the raw bytes after the headers are an exact body
 * prefix.
 */
@experimental
object ParityH1IO {

  private val UsAscii        = StandardCharsets.US_ASCII
  private val PartialMaxRead = 4096

  def partialGet(in: InputStream, out: OutputStream, path: String): AbortedRead = {
    out.write(("GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").getBytes(UsAscii))
    out.flush()
    val (status, headers) = readStatusAndHeaders(in)
    val length            = headers
      .get("content-length")
      .flatMap(_.headOption.map(_.trim))
      .map(_.toInt)
      .getOrElse(throw new AssertionError("Cancellation target must carry Content-Length on H1"))
    val want              = Math.min(length, PartialMaxRead)
    val buf               = new Array[Byte](want)
    var offset            = 0
    while (offset < want) {
      val n = in.read(buf, offset, want - offset)
      if (n < 0) throw new EOFException("EOF inside partial H1 body")
      offset += n
    }
    AbortedRead(status, headers, Chunk.fromArray(java.util.Arrays.copyOf(buf, offset)))
  }

  /**
   * HEAD response reader: status line plus headers with an empty body. The
   * server advertises Content-Length for the representation that a GET would
   * return but sends zero body bytes, so reading a body would block to EOF.
   */
  def readHeadResponse(in: InputStream): ObservedResponse = {
    val (status, headers) = readStatusAndHeaders(in)
    ObservedResponse(status, headers, Chunk.empty[Byte])
  }

  private def readStatusAndHeaders(in: InputStream): (Int, Map[String, List[String]]) = {
    val statusLine = readLine(in)
    val parts      = statusLine.split(" ", 3)
    if (parts.length < 2 || !parts(0).startsWith("HTTP/"))
      throw new AssertionError("Bad H1 status line: " + statusLine)
    var headers    = Map.empty[String, List[String]]
    var line       = readLine(in)
    while (line.nonEmpty) {
      val colon = line.indexOf(':')
      if (colon < 0) throw new AssertionError("Bad H1 header line: " + line)
      val name  = line.substring(0, colon).trim.toLowerCase(Locale.ROOT)
      val value = line.substring(colon + 1).trim
      headers = headers.updated(name, headers.getOrElse(name, Nil) :+ value)
      line = readLine(in)
    }
    (parts(1).toInt, headers)
  }

  private def readLine(in: InputStream): String = {
    val buf   = new java.io.ByteArrayOutputStream()
    var done  = false
    while (!done) {
      val b = in.read()
      if (b < 0) throw new EOFException("EOF inside H1 response line")
      if (b == '\n') done = true
      else buf.write(b)
    }
    val bytes = buf.toByteArray
    val len   = if (bytes.length > 0 && bytes(bytes.length - 1) == '\r') bytes.length - 1 else bytes.length
    new String(bytes, 0, len, UsAscii)
  }
}

/**
 * Todo 16: raw TLS+H2 partial read — preface and SETTINGS over an
 * ALPN-negotiated `h2` TLS stream, one GET, then the first DATA frame. Window
 * top-up mirrors a real receiver so the paced server never stalls.
 */
@experimental
object ParityTlsH2IO {

  private val UsAscii         = StandardCharsets.US_ASCII
  private val PrefaceBytes    = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(UsAscii)
  private val SocketTimeoutMs = 20000

  def partialGet(port: Int, path: String): AbortedRead = {
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(SocketTimeoutMs)
    val sslSocket = TlsAlpnFixtures
      .trustAllContext()
      .getSocketFactory
      .createSocket(rawSocket, "127.0.0.1", port, true)
      .asInstanceOf[javax.net.ssl.SSLSocket]
    try {
      val params = sslSocket.getSSLParameters
      params.setApplicationProtocols(Array("h2"))
      sslSocket.setSSLParameters(params)
      sslSocket.setUseClientMode(true)
      sslSocket.startHandshake()
      if (sslSocket.getApplicationProtocol != "h2")
        throw new AssertionError("Expected h2 ALPN but negotiated: " + sslSocket.getApplicationProtocol)
      val conn   = new PartialConnection(sslSocket.getInputStream, sslSocket.getOutputStream, port)
      conn.handshake()
      conn.getThenAbort(path)
    } finally {
      try sslSocket.close()
      catch { case _: Throwable => () }
    }
  }

  private final class PartialConnection(in: InputStream, out: OutputStream, port: Int) {
    private var buf     = Chunk.empty[Byte]
    private val encoder = new HpackEncoder()
    private val decoder = new HpackDecoder()

    def handshake(): Unit = {
      out.write(PrefaceBytes)
      out.write(FrameCodec.encode(Settings(ack = false, Nil)).toArray)
      out.flush()
      readFrame() match {
        case Settings(false, _) => sendFrame(Settings(ack = true, Nil))
        case other              => throw new AssertionError("Expected server SETTINGS but received: " + other)
      }
      readFrame()
    }

    def getThenAbort(path: String): AbortedRead = {
      val pseudo = List(
        HeaderField(":method", "GET"),
        HeaderField(":path", path),
        HeaderField(":scheme", "https"),
        HeaderField(":authority", "127.0.0.1:" + port),
      )
      sendFrame(Headers(streamId = 1, headerBlock = encoder.encode(pseudo), endStream = true, endHeaders = true))
      val hdrs   = ListBuffer.empty[HeaderField]
      var status = -1
      var first  = Chunk.empty[Byte]
      var haveIt = false
      while (!haveIt) {
        readFrame() match {
          case Settings(false, _)                            => sendFrame(Settings(ack = true, Nil))
          case Settings(true, _)                             => ()
          case _: WindowUpdate                               => ()
          case Ping(false, data)                             => sendFrame(Ping(ack = true, data))
          case Headers(sid, block, end, _, _, _) if sid == 1 =>
            val fields = decode(block)
            hdrs ++= fields
            status = fields.find(_.name == ":status").map(_.value.toInt).getOrElse(status)
            if (end) haveIt = true
          case Headers(_, block, _, _, _, _)                 =>
            decode(block)
          case Data(sid, data, _, _) if sid == 1             =>
            first = first ++ data
            topUp(sid, data.length)
            haveIt = true
          case Data(sid, data, _, _)                         =>
            topUp(sid, data.length)
          case GoAway(_, code, debug)                        =>
            throw new AssertionError("Unexpected GOAWAY: " + code + " " + new String(debug.toArray, UsAscii))
          case _                                             => ()
        }
      }
      if (status < 0) throw new AssertionError("Partial TLS-H2 response carried no :status header")
      AbortedRead(status, headerMap(hdrs.toList), first)
    }

    private def decode(block: Chunk[Byte]): List[HeaderField] =
      decoder.decode(block) match {
        case Right(fields) => fields
        case Left(error)   => throw new AssertionError("HPACK decode failed: " + error)
      }

    private def headerMap(fields: List[HeaderField]): Map[String, List[String]] =
      fields.groupMap(_.name.toLowerCase(Locale.ROOT))(_.value)

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

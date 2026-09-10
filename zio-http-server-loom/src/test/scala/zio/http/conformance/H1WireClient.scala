package zio.http.conformance

import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets

import scala.annotation.experimental

import zio.blocks.chunk.Chunk

/**
 * Todo 10: real HTTP/1.1 test client (server half arrives with Todo 7).
 *
 * Speaks request line + headers + optional body over any byte streams and
 * parses status line, headers, and all three body framings (Content-Length,
 * chunked with trailers, close-delimited). [[H1BackendReadinessSpec]] covers it
 * against canned loopback bytes; Todo 7/16 point it at the real H1 engine with
 * zero corpus changes.
 */
@experimental
final class H1WireClient(in: InputStream, out: OutputStream) {

  private val UsAscii = StandardCharsets.US_ASCII
  private val Utf8    = StandardCharsets.UTF_8

  def sendRequest(
    method: String,
    path: String,
    headers: List[(String, String)],
    body: Chunk[Byte],
  ): Unit = {
    val head    = new StringBuilder()
    head.append(method).append(' ').append(path).append(" HTTP/1.1\r\n")
    var hasHost = false
    var i       = 0
    while (i < headers.length) {
      if (headers(i)._1.equalsIgnoreCase("host")) hasHost = true
      i += 1
    }
    if (!hasHost) head.append("Host: 127.0.0.1\r\n")
    headers.foreach { case (name, value) => head.append(name).append(": ").append(value).append("\r\n") }
    if (body.nonEmpty) head.append("Content-Length: ").append(body.length.toString).append("\r\n")
    head.append("Connection: close\r\n\r\n")
    out.write(head.toString.getBytes(UsAscii))
    if (body.nonEmpty) out.write(body.toArray)
    out.flush()
  }

  def readResponse(): ObservedResponse = {
    val statusLine = readLine()
    val parts      = statusLine.split(" ", 3)
    if (parts.length < 2 || !parts(0).startsWith("HTTP/")) throw new AssertionError("Bad status line: " + statusLine)
    val status     = parts(1).toInt
    var headers    = Map.empty[String, List[String]]
    var line       = readLine()
    while (line.nonEmpty) {
      val colon = line.indexOf(':')
      if (colon < 0) throw new AssertionError("Bad header line: " + line)
      val name  = line.substring(0, colon).trim.toLowerCase(java.util.Locale.ROOT)
      val value = line.substring(colon + 1).trim
      headers = headers.updated(name, headers.getOrElse(name, Nil) :+ value)
      line = readLine()
    }
    val body       =
      if (headers.get("transfer-encoding").exists(_.exists(_.toLowerCase(java.util.Locale.ROOT).contains("chunked"))))
        readChunked()
      else
        headers.get("content-length").flatMap(_.headOption.map(_.trim)) match {
          case Some(length) => readExact(length.toInt)
          case None         => readUntilClose()
        }
    ObservedResponse(status, headers, body)
  }

  private def readChunked(): Chunk[Byte] = {
    var body = Chunk.empty[Byte]
    var done = false
    while (!done) {
      val line     = readLine()
      val semi     = line.indexOf(';')
      val sizeText = (if (semi < 0) line else line.substring(0, semi)).trim
      val size     = Integer.parseInt(sizeText, 16)
      if (size == 0) {
        var trailer = readLine()
        while (trailer.nonEmpty) trailer = readLine()
        done = true
      } else {
        body = body ++ Chunk.fromArray(readExactBytes(size))
        val crlf = readExactBytes(2)
        if (crlf(0) != '\r' || crlf(1) != '\n') throw new AssertionError("Chunk missing CRLF")
      }
    }
    body
  }

  private def readExact(length: Int): Chunk[Byte] =
    Chunk.fromArray(readExactBytes(length))

  private def readExactBytes(length: Int): Array[Byte] = {
    val out    = new Array[Byte](length)
    var offset = 0
    while (offset < length) {
      val n = in.read(out, offset, length - offset)
      if (n < 0) throw new java.io.EOFException("EOF after " + offset + " of " + length + " bytes")
      offset += n
    }
    out
  }

  private def readUntilClose(): Chunk[Byte] = {
    var body = Chunk.empty[Byte]
    val tmp  = new Array[Byte](8192)
    var n    = in.read(tmp)
    while (n >= 0) {
      body = body ++ Chunk.fromArray(java.util.Arrays.copyOf(tmp, n))
      n = in.read(tmp)
    }
    body
  }

  private def readLine(): String = {
    val buf   = new java.io.ByteArrayOutputStream()
    var done  = false
    while (!done) {
      val b = in.read()
      if (b < 0) throw new java.io.EOFException("EOF inside response line")
      if (b == '\n') done = true
      else buf.write(b)
    }
    val bytes = buf.toByteArray
    val len   = if (bytes.length > 0 && bytes(bytes.length - 1) == '\r') bytes.length - 1 else bytes.length
    new String(bytes, 0, len, UsAscii)
  }
}

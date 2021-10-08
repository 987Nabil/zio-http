package zhttp.http

import io.netty.handler.codec.http.HttpHeaderNames
import zhttp.experiment.ContentDecoder
import zio.{Chunk, ZIO}
import java.net.InetAddress

import zhttp.socket.SocketApp

trait Request extends HeadersHelpers { self =>
  def method: Method

  def url: URL

  def headers: List[Header]

  def path: Path = url.path

  def decodeContent[R, B](decoder: ContentDecoder[R, Throwable, Chunk[Byte], B]): ZIO[R, Throwable, B]

  def remoteAddress: Option[InetAddress]

  def addHeader(header: Header): Request = self.copy(headers = header :: self.headers)

  def removeHeader(name: CharSequence): Request = self.copy(headers = self.headers.filter(_.name != name))

  def copy(method: Method = self.method, url: URL = self.url, headers: List[Header] = self.headers): Request = {
    val m = method
    val u = url
    val h = headers
    new Request {
      override def method: Method = m

      override def url: URL = u

      override def headers: List[Header] = h

      override def remoteAddress: Option[InetAddress] =
        self.remoteAddress

      override def decodeContent[R, B](decoder: ContentDecoder[R, Throwable, Chunk[Byte], B]): ZIO[R, Throwable, B] =
        self.decodeContent(decoder)
    }
  }
  private def checkWebSocketUpgrade: Boolean = self.getHeaderValue(HttpHeaderNames.UPGRADE) match {
    case Some(value) if value.toLowerCase equals "websocket" => true
    case Some(_)                                             => false
    case None                                                => false
  }
  private def checkWebSocketKey: Boolean     = self getHeaderValue HttpHeaderNames.SEC_WEBSOCKET_KEY match {
    case Some(_) => true
    case None    => false
  }
  private def checkWebSocketMethod: Boolean  = self.method.equals(Method.GET)

  def isValidWebSocketRequest: Boolean =
    checkWebSocketKey && checkWebSocketUpgrade && checkWebSocketMethod

  def fromSocketApp[R, E](socketApp: SocketApp[R, E]): Response[R, E] =
    SocketResponse.from(req = self, socketApp = socketApp)
}

object Request {
  def apply(method: Method = Method.GET, url: URL = URL.root, headers: List[Header] = Nil): Request = {
    val m = method
    val u = url
    val h = headers
    new Request {
      override def method: Method = m

      override def url: URL = u

      override def headers: List[Header] = h

      override def remoteAddress: Option[InetAddress] = None

      override def decodeContent[R, B](decoder: ContentDecoder[R, Throwable, Chunk[Byte], B]): ZIO[R, Throwable, B] =
        ZIO.fail(ContentDecoder.Error.DecodeEmptyContent)
    }
  }
}

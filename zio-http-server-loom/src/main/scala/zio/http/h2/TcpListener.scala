package zio.http.h2

import java.io.{InputStream, OutputStream}
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext

import zio.http.{LoomBoundListener, LoomListener, TlsConfig}

/**
 * Identity of the TCP peer accepted by [[TcpListener]], forwarded to the
 * connection handler so request decoding can gate forwarding headers on it.
 *
 * @param address
 *   Socket peer IP address (for example `"127.0.0.1"`).
 * @param peerCert
 *   Verified client certificate, present only on mTLS connections whose peer
 *   completed client authentication.
 */
private[http] final case class PeerInfo(
  address: String,
  peerCert: Option[X509Certificate],
) {
  def hasPeerCert: Boolean = peerCert.isDefined
}

/**
 * H2 adapter over the protocol-independent [[LoomListener]].
 *
 * Preserves the historical `(InputStream, OutputStream, PeerInfo)` handler
 * shape and the `BoundListener`/`createSslContext` entry points so the
 * pre-existing H2 suite keeps working unchanged; all TCP accept, TLS setup,
 * virtual-thread connection ownership, binding and active-resource management
 * now live in the generic listener. The adapter performs no framing itself —
 * the handler still owns the H2 wire semantics.
 */
private[http] class TcpListener(
  host: String,
  port: Int,
  tls: Option[TlsConfig],
  connectionHandler: (InputStream, OutputStream, PeerInfo) => Unit,
) {

  private val delegate = new LoomListener(
    host,
    port,
    tls,
    conn => connectionHandler(conn.input, conn.output, PeerInfo(conn.peer.address, conn.peer.peerCert)),
  )

  def start(): BoundListener = {
    val bound = delegate.start()
    BoundListener(bound.host, bound.port, bound.close, bound.isRunning)
  }
}

case class BoundListener(
  host: String,
  port: Int,
  close: () => Unit,
  isRunning: () => Boolean,
)

private[http] object TcpListener {

  def createSslContext(tls: TlsConfig): SSLContext =
    LoomListener.createSslContext(tls)

  def toGeneric(bound: BoundListener): LoomBoundListener =
    LoomBoundListener(bound.host, bound.port, bound.close, bound.isRunning)
}

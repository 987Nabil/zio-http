package zio.http

import java.io.{ByteArrayInputStream, InputStream, OutputStream}
import java.net.InetSocketAddress
import java.nio.channels.{Channels, ServerSocketChannel, SocketChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.KeyStore
import java.security.cert.{Certificate, CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey, SecureRandom}
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import javax.net.ssl.{
  KeyManagerFactory,
  SSLContext,
  SSLHandshakeException,
  SSLParameters,
  SSLPeerUnverifiedException,
  SSLSocket,
  TrustManager,
  TrustManagerFactory,
}

import scala.jdk.CollectionConverters._
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import zio.blocks.config.Secret
import zio.blocks.telemetry.{AttributeValue, ConsoleLogRecordProcessor, LoggerProvider}

/**
 * Identity of the TCP peer accepted by [[LoomListener]], forwarded to the
 * connection handler so request decoding can gate forwarding headers on it.
 *
 * @param address
 *   Socket peer IP address (for example `"127.0.0.1"`).
 * @param peerCert
 *   Verified client certificate, present only on mTLS connections whose peer
 *   completed client authentication.
 */
private[http] final case class AcceptedPeer(
  address: String,
  peerCert: Option[X509Certificate],
) {
  def hasPeerCert: Boolean = peerCert.isDefined
}

/**
 * One connection accepted by [[LoomListener]].
 *
 * The listener owns TCP accept, TLS setup, virtual-thread hosting, binding and
 * active-resource tracking, and stops here: it performs no framing, no routing
 * and no protocol dispatch. The negotiated ALPN identifier (when the peer
 * offered one over TLS) and the peer identity are surfaced as metadata so a
 * later dispatch layer can select an engine.
 *
 * @param input
 *   Plaintext byte source of the connection (decrypted when [[secure]]).
 * @param output
 *   Plaintext byte sink of the connection (encrypted when [[secure]]).
 * @param peer
 *   Accepted peer identity.
 * @param negotiatedAlpn
 *   ALPN protocol negotiated during the TLS handshake; empty on cleartext
 *   connections and when the peer offered no ALPN extension.
 * @param secure
 *   True when the connection completed a TLS handshake.
 */
private[http] final case class AcceptedConnection(
  input: InputStream,
  output: OutputStream,
  peer: AcceptedPeer,
  negotiatedAlpn: Option[String],
  secure: Boolean,
)

/**
 * Protocol-independent Loom listener.
 *
 * Extracted from the former H2-only TCP listener: accepts TCP connections on a
 * virtual-thread acceptor, optionally completes a TLS handshake with the
 * configured ALPN list, policy, version pins and client-authentication mode,
 * hosts every connection on its own virtual thread, and tracks bound and active
 * resources for coordinated close. Coordination uses signaled
 * `java.util.concurrent` primitives (atomics, concurrent sets, interrupt/join);
 * no monitor `synchronized` blocks and no sleep polling sit on the accept or
 * connection hot paths.
 *
 * @param connectionHandler
 *   Handles one accepted connection synchronously on the connection's virtual
 *   thread. Returning from the handler closes the connection.
 */
private[http] class LoomListener(
  host: String,
  port: Int,
  tls: Option[TlsConfig],
  connectionHandler: AcceptedConnection => Unit,
) {
  private val logger =
    LoggerProvider.builder.addLogRecordProcessor(new ConsoleLogRecordProcessor).build().get("zio.http.LoomListener")

  def start(): LoomBoundListener = {
    val serverChannel     = ServerSocketChannel.open()
    val running           = new AtomicBoolean(true)
    val connectionCounter = new AtomicLong(0L)
    val activeConnections = ConcurrentHashMap.newKeySet[AutoCloseable]()
    val sslContext        = tls.map(LoomListener.createSslContext)

    try {
      serverChannel.bind(new InetSocketAddress(host, port))
    } catch {
      case NonFatal(e) =>
        closeQuietly(serverChannel)
        throw e
    }

    val acceptor = Thread
      .ofVirtual()
      .name(s"zio-http-$host:$port")
      .start(() => acceptLoop(serverChannel, running, activeConnections, connectionCounter, sslContext, tls))

    val localAddress = serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress]

    // Stop-accept (Todo 8): close the server channel and join the acceptor so
    // no new TCP connection is accepted, while established connections stay
    // alive for the engine drain phase. Connection release stays single-shot:
    // a repeated close never re-closes resources the first close released.
    val stopAccept: () => Unit = () => {
      if (running.compareAndSet(true, false)) {
        closeQuietly(serverChannel)
        acceptor.interrupt()
        acceptor.join()
      }
    }
    val releasedConnections    = new AtomicBoolean(false)
    val closeAll0: () => Unit  = () => {
      stopAccept()
      if (releasedConnections.compareAndSet(false, true)) closeAll(activeConnections)
    }

    LoomBoundListener(
      localAddress.getHostString,
      localAddress.getPort,
      closeAll0,
      () => running.get() && acceptor.isAlive && serverChannel.isOpen,
      stopAccept,
    )
  }

  private def acceptLoop(
    serverChannel: ServerSocketChannel,
    running: AtomicBoolean,
    activeConnections: java.util.Set[AutoCloseable],
    connectionCounter: AtomicLong,
    sslContext: Option[SSLContext],
    tls: Option[TlsConfig],
  ): Unit = {
    while (running.get() && serverChannel.isOpen) {
      try {
        val channel      = serverChannel.accept()
        val connectionId = connectionCounter.incrementAndGet()
        Thread
          .ofVirtual()
          .name(s"zio-http-conn-$connectionId")
          .start(() => handleConnection(channel, activeConnections, sslContext, tls))
      } catch {
        case _: java.nio.channels.AsynchronousCloseException if !running.get() || !serverChannel.isOpen => ()
        case _: java.net.SocketException if !running.get() || !serverChannel.isOpen                     => ()
        case NonFatal(e)                                                                                =>
          logger.error(
            "Loom accept loop error",
            "host"          -> AttributeValue.StringValue(host),
            "port"          -> AttributeValue.LongValue(port.toLong),
            "error_type"    -> AttributeValue.StringValue(e.getClass.getSimpleName),
            "error_message" -> AttributeValue.StringValue(Option(e.getMessage).getOrElse("")),
            "stacktrace"    -> AttributeValue.StringValue(stackTraceToString(e)),
          )
      }
    }
  }

  private def stackTraceToString(e: Throwable): String = {
    val sw = new java.io.StringWriter()
    e.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  private def handleConnection(
    channel: SocketChannel,
    activeConnections: java.util.Set[AutoCloseable],
    sslContext: Option[SSLContext],
    tls: Option[TlsConfig],
  ): Unit = {
    sslContext match {
      case Some(context) =>
        // A peer that fails the handshake (including mTLS client authentication) never reaches the
        // handler: the socket and channel are closed here so the peer observes the rejection
        // promptly.
        var sslSocket: SSLSocket = null
        try {
          sslSocket = LoomListener.createTlsSocket(context, channel, tls)
          activeConnections.add(sslSocket)
          try {
            val peer =
              AcceptedPeer(LoomListener.peerIp(sslSocket.getRemoteSocketAddress), LoomListener.peerCert(sslSocket))
            val alpn = Option(sslSocket.getApplicationProtocol).filter(_.nonEmpty)
            connectionHandler(AcceptedConnection(sslSocket.getInputStream, sslSocket.getOutputStream, peer, alpn, true))
          } finally {
            activeConnections.remove(sslSocket)
            closeQuietly(sslSocket)
          }
        } catch {
          case NonFatal(e) =>
            logger.error(
              "Loom TLS handshake failed",
              "host"          -> AttributeValue.StringValue(host),
              "port"          -> AttributeValue.LongValue(port.toLong),
              "error_type"    -> AttributeValue.StringValue(e.getClass.getSimpleName),
              "error_message" -> AttributeValue.StringValue(Option(e.getMessage).getOrElse("")),
            )
            closeQuietly(sslSocket)
            closeQuietly(channel)
        }
      case None          =>
        activeConnections.add(channel)
        try {
          val peer = AcceptedPeer(LoomListener.peerIp(channel.getRemoteAddress), None)
          connectionHandler(
            AcceptedConnection(Channels.newInputStream(channel), Channels.newOutputStream(channel), peer, None, false),
          )
        } finally {
          activeConnections.remove(channel)
          closeQuietly(channel)
        }
    }
  }

  private def closeAll(resources: java.util.Set[AutoCloseable]): Unit =
    resources.asScala.foreach(closeQuietly)

  private def closeQuietly(resource: AutoCloseable): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case NonFatal(_) => ()
      }
    }
}

/** Binding returned by [[LoomListener.start]]. */
case class LoomBoundListener(
  host: String,
  port: Int,
  close: () => Unit,
  isRunning: () => Boolean,
  /**
   * Stop accepting new connections while established ones stay alive (Todo 8:
   * the aggregate drain phase calls this before draining the engines; `close`
   * still force-closes everything).
   */
  stopAccepting: () => Unit,
)

private[http] object LoomListener {
  private val PrivateKeyHeader = "-----BEGIN PRIVATE KEY-----"
  private val PrivateKeyFooter = "-----END PRIVATE KEY-----"
  private val RsaKeyHeader     = "-----BEGIN RSA PRIVATE KEY-----"
  private val RsaKeyFooter     = "-----END RSA PRIVATE KEY-----"

  def createSslContext(tls: TlsConfig): SSLContext =
    firstSslContext(tls).getOrElse {
      val certificates = loadCertificates(tls.certChain)
      val privateKey   = loadPrivateKey(tls.privateKey)
      val keyStore     = KeyStore.getInstance(KeyStore.getDefaultType)
      val password     = Array.emptyCharArray

      keyStore.load(null, password)
      keyStore.setKeyEntry("zio-http-server-loom", privateKey, password, certificates.map(identity[Certificate]))

      val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      keyManagerFactory.init(keyStore, password)

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(keyManagerFactory.getKeyManagers, trustManagers(tls), new SecureRandom())
      requireSupportedProtocols(sslContext, tls, "PEM cert/key material")
      sslContext
    }

  /**
   * Builds the trust managers for server-side client-certificate verification.
   * Always real managers, never `null`: an explicit `trustCertChain` builds a
   * CA store from it, otherwise the platform default trust store is used.
   */
  private def trustManagers(tls: TlsConfig): Array[TrustManager] =
    tls.trustCertChain match {
      case Some(source) =>
        val certificates        = loadCertificates(source)
        val trustStore          = KeyStore.getInstance(KeyStore.getDefaultType)
        trustStore.load(null, null)
        certificates.zipWithIndex.foreach { case (certificate, index) =>
          trustStore.setCertificateEntry("zio-http-trust-ca-" + index, certificate)
        }
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        trustManagerFactory.init(trustStore)
        trustManagerFactory.getTrustManagers
      case None         =>
        val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        trustManagerFactory.init(null: KeyStore)
        trustManagerFactory.getTrustManagers
    }

  def createTlsSocket(sslContext: SSLContext, channel: SocketChannel, tls: Option[TlsConfig]): SSLSocket = {
    val socket = sslContext.getSocketFactory
      .createSocket(channel.socket(), channel.socket().getInetAddress.getHostAddress, channel.socket().getPort, true)
      .asInstanceOf[SSLSocket]

    // ALPN comes from TlsConfig on every path (including the firstSslContext
    // bypass, which only skips keystore loading): per-socket parameters are
    // applied here, downstream of either SSLContext source. Pinned TLS
    // versions are enforced the same way.
    val alpnProtocols = tls.map(_.alpnProtocols).getOrElse(List("h2"))
    val alpnPolicy    = tls.map(_.alpnPolicy).getOrElse(AlpnPolicy.StrictH2)
    val tlsVersions   = tls.map(_.tlsVersions).getOrElse(List("TLSv1.3", "TLSv1.2"))

    val sslEngine  = sslContext.createSSLEngine()
    val parameters = withAlpn(sslEngine.getSSLParameters, alpnProtocols)
    if (tlsVersions.nonEmpty) parameters.setProtocols(tlsVersions.toArray)

    sslEngine.setUseClientMode(false)
    sslEngine.setSSLParameters(parameters)

    socket.setUseClientMode(false)
    socket.setSSLParameters(parameters)
    if (tls.exists(_.requireClientAuth)) socket.setNeedClientAuth(true)
    // Every reject path below closes explicitly: on the http/1.1-only path
    // the JDK fails inside startHandshake BEFORE any post-handshake close
    // could run, which left FIN to socket GC (CLOSE_WAIT window + noisy
    // trace). Handshake semantics are unchanged — the same exception instance
    // propagates after the explicit close.
    try {
      socket.startHandshake()

      val negotiatedProtocol = socket.getApplicationProtocol
      alpnPolicy match {
        case AlpnPolicy.StrictH2 if negotiatedProtocol != "h2" =>
          throw new SSLHandshakeException(s"Expected ALPN protocol 'h2' but negotiated '$negotiatedProtocol'")
        case _                                                 => ()
      }
    } catch {
      case NonFatal(e) =>
        closeQuietly(socket)
        throw e
    }

    socket
  }

  private def withAlpn(parameters: SSLParameters, alpnProtocols: List[String]): SSLParameters = {
    parameters.setApplicationProtocols(alpnProtocols.toArray)
    parameters
  }

  /**
   * Socket peer IP for proxy-trust gating; falls back to the raw address
   * string.
   */
  private[http] def peerIp(remote: java.net.SocketAddress): String =
    remote match {
      case inet: InetSocketAddress if inet.getAddress != null => inet.getAddress.getHostAddress
      case other                                              => String.valueOf(other)
    }

  /**
   * Verified client certificate of an mTLS peer, if the handshake authenticated
   * one.
   */
  private[http] def peerCert(socket: SSLSocket): Option[X509Certificate] =
    try socket.getSession.getPeerCertificates.collectFirst { case certificate: X509Certificate => certificate }
    catch {
      case _: SSLPeerUnverifiedException => None
    }

  private def firstSslContext(tls: TlsConfig): Option[SSLContext] =
    tls.certChain match {
      case TlsSource.SslContext(ctx) => Some(requireProvidedContext(ctx, tls, "certChain"))
      case _                         =>
        tls.privateKey match {
          case TlsSource.SslContext(ctx) => Some(requireProvidedContext(ctx, tls, "privateKey"))
          case _                         => None
        }
    }

  /**
   * A caller-provided SSLContext carries key material but never the configured
   * ALPN list or the pinned TLS versions (both are per-socket SSLParameters,
   * applied downstream in createTlsSocket): return it for the keystore bypass,
   * but fail fast when the TlsConfig it would silently ignore is misconfigured
   * or unsupported.
   */
  private def requireProvidedContext(ctx: SSLContext, tls: TlsConfig, field: String): SSLContext = {
    if (tls.alpnProtocols.isEmpty)
      throw new IllegalArgumentException(
        s"ALPN not configured on provided SSLContext ($field): TlsConfig.alpnProtocols is empty; " +
          "configure alpnProtocols (e.g. List(\"h2\")) so the server can wrap the provided SSLContext, " +
          "or provide PEM cert/key material instead",
      )
    requireSupportedProtocols(ctx, tls, s"provided SSLContext ($field)")
    ctx
  }

  private def requireSupportedProtocols(ctx: SSLContext, tls: TlsConfig, source: String): Unit = {
    require(
      tls.tlsVersions.nonEmpty,
      "TlsConfig.tlsVersions must not be empty; pin e.g. List(\"TLSv1.3\", \"TLSv1.2\")",
    )
    val supported = ctx.getSupportedSSLParameters.getProtocols.toSet
    val missing   = tls.tlsVersions.filterNot(supported.contains)
    if (missing.nonEmpty)
      throw new IllegalArgumentException(
        s"TLS protocol versions ${missing.mkString("[", ",", "]")} not supported by $source; " +
          s"supported: ${supported.toList.sorted.mkString("[", ",", "]")}",
      )
  }

  private def loadCertificates(source: TlsSource): Array[X509Certificate] = {
    val bytes        = readSourceBytes(source)
    val certificates =
      CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(bytes)).asScala

    certificates.collect { case certificate: X509Certificate => certificate }.toArray
  }

  private def loadPrivateKey(source: TlsSource): PrivateKey = {
    val pem = readSourceString(source)

    parsePemBlock(pem, PrivateKeyHeader, PrivateKeyFooter) match {
      case Some(pkcs8Bytes) => parsePkcs8PrivateKey(pkcs8Bytes)
      case None             =>
        parsePemBlock(pem, RsaKeyHeader, RsaKeyFooter) match {
          case Some(pkcs1Bytes) => parsePkcs8PrivateKey(wrapPkcs1RsaKey(pkcs1Bytes))
          case None             => throw new IllegalArgumentException("Unsupported private key PEM format")
        }
    }
  }

  private def parsePkcs8PrivateKey(keyBytes: Array[Byte]): PrivateKey = {
    val spec = new PKCS8EncodedKeySpec(keyBytes)

    List("RSA", "EC", "DSA", "Ed25519", "Ed448").iterator
      .map(algorithm => Try(KeyFactory.getInstance(algorithm).generatePrivate(spec)))
      .collectFirst { case Success(privateKey) => privateKey }
      .getOrElse(throw new IllegalArgumentException("Unable to parse private key with supported algorithms"))
  }

  private def readSourceBytes(source: TlsSource): Array[Byte] =
    source match {
      case TlsSource.FilePath(path)    => Files.readAllBytes(path)
      case TlsSource.PemString(secret) => Secret.unwrap(secret).getBytes(StandardCharsets.UTF_8)
      case TlsSource.SslContext(_) => throw new IllegalArgumentException("SSLContext source does not expose PEM bytes")
    }

  private def readSourceString(source: TlsSource): String =
    new String(readSourceBytes(source), StandardCharsets.UTF_8)

  private def parsePemBlock(pem: String, header: String, footer: String): Option[Array[Byte]] = {
    val start = pem.indexOf(header)
    val end   = pem.indexOf(footer)

    if (start < 0 || end < 0 || end <= start) None
    else {
      val base64 = pem
        .substring(start + header.length, end)
        .replaceAll("\\s", "")

      Some(Base64.getDecoder.decode(base64))
    }
  }

  private def wrapPkcs1RsaKey(pkcs1Bytes: Array[Byte]): Array[Byte] = {
    val rsaAlgorithmIdentifier = Array[Byte](
      0x30.toByte,
      0x0d.toByte,
      0x06.toByte,
      0x09.toByte,
      0x2a.toByte,
      0x86.toByte,
      0x48.toByte,
      0x86.toByte,
      0xf7.toByte,
      0x0d.toByte,
      0x01.toByte,
      0x01.toByte,
      0x01.toByte,
      0x05.toByte,
      0x00.toByte,
    )
    val version                = Array[Byte](0x02.toByte, 0x01.toByte, 0x00.toByte)
    val privateKeyOctetString  = derEncode(0x04, pkcs1Bytes)
    val privateKeyInfoContent  = version ++ rsaAlgorithmIdentifier ++ privateKeyOctetString

    derEncode(0x30, privateKeyInfoContent)
  }

  private def derEncode(tag: Int, content: Array[Byte]): Array[Byte] =
    Array(tag.toByte) ++ derLength(content.length) ++ content

  private def derLength(length: Int): Array[Byte] =
    if (length < 128) Array(length.toByte)
    else {
      val bytes = BigInt(length).toByteArray.dropWhile(_ == 0.toByte)
      Array((0x80 | bytes.length).toByte) ++ bytes
    }

  private def closeQuietly(resource: AutoCloseable): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case NonFatal(_) => ()
      }
    }
}

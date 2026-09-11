package zio.http

import java.net.Socket
import javax.net.ssl.{SSLContext, SSLSocket, TrustManager, X509TrustManager}
import javax.net.ssl.X509ExtendedTrustManager
import java.security.cert.X509Certificate
import javax.net.ssl.SSLEngine

import scala.annotation.experimental

import zio.blocks.config.Secret

/**
 * Todo 11: shared TLS test material for the ALPN dispatch specs.
 *
 * One throwaway self-signed loopback certificate (same shape as the H2 TLS
 * specs use), a trust-all client context, and raw-handshake helpers. Blocking
 * socket IO always runs off the ZIO executor via `ZIO.attemptBlocking` at the
 * call site; every socket carries a bounded `SoTimeout`, never a sleep.
 */
@experimental
private[http] object TlsAlpnFixtures {

  val TestCert =
    """-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIIFmlxlymbftowDQYJKoZIhvcNAQEMBQAwXTELMAkGA1UE
BhMCVVMxDTALBgNVBAgTBFRlc3QxDTALBgNVBAcTBFRlc3QxDTALBgNVBAoTBFRl
c3QxDTALBgNVBAsTBFRlc3QxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0yNjA2MzAy
MjA1NTlaFw0yNzA2MzAyMjA1NTlaMF0xCzAJBgNVBAYTAlVTMQ0wCwYDVQQIEwRU
ZXN0MQ0wCwYDVQQHEwRUZXN0MQ0wCwYDVQQKEwRUZXN0MQ0wCwYDVQQLEwRUZXN0
MRIwEAYDVQQDEwlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDihzLu4ln5ta1Rgac4J3GsWMLWVjMoud5NiZczB7RLHQx+yt1uYhDqc8HT
gzkEBU8lel1IdEMP+m4Y/tVZLrjMaH6lvjbLSQLjdgIsvtqeHmTfMBcCTr9E+r4k
Lhtc1utAOpL18DPBxXEQ7ib2MAtxjLXJQIU/Zh4GJNfbJ69IjFF/PTZUZsIWmJxB
zR9M+2NN1y0gtH6FpdQepQeFaeCJ43652NIKGAuM/w4G2DYSBUsHb/WsMc5QZm0M
DQ6Gy9E76jyghywdkUPw7dnioqzUhbCIZ8eXiL4YbJ6n9eeWvVrGyAWGBYstYEJq
OrR2KbGcd57R3ZvAkPcHIdIy+WO9AgMBAAGjITAfMB0GA1UdDgQWBBT8SbyNOu1I
6jQLbe4/D888GtPHeTANBgkqhkiG9w0BAQwFAAOCAQEAoa31bUJ541BZn321u3K8
XYfdFlTy3zLF4E7OlC9ygepgMKFmntQOnfg19rKgZO+VkQ8kBusgo/jiavjrQIDw
2tTwKel+kN1STaLt5xEWMQsGbGvT7iSejin3wFSxMIrbWKtSOc3Li0AmbdERJ37L
QAYSxLK+vU4BTT/whdI223xeFLQGYFhMyTag09Osw1WLUUZRvLh1FPeV5P5dWpzc
dpBrxWvWGRl2+Nle0zAQNznhfn8ydP/7K3Lv/f3qQ48EgXmSDdhvSUfX24CLVLXk
mETcQb+xmaWObOxkaK0iWGoQWPs2UFKVHPDmUlsYSt0ePGsiu1uEbgWrfr+6vBdx
Wg==
-----END CERTIFICATE-----"""

  val TestKey =
    """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDihzLu4ln5ta1R
gac4J3GsWMLWVjMoud5NiZczB7RLHQx+yt1uYhDqc8HTgzkEBU8lel1IdEMP+m4Y
/tVZLrjMaH6lvjbLSQLjdgIsvtqeHmTfMBcCTr9E+r4kLhtc1utAOpL18DPBxXEQ
7ib2MAtxjLXJQIU/Zh4GJNfbJ69IjFF/PTZUZsIWmJxBzR9M+2NN1y0gtH6FpdQe
pQeFaeCJ43652NIKGAuM/w4G2DYSBUsHb/WsMc5QZm0MDQ6Gy9E76jyghywdkUPw
7dnioqzUhbCIZ8eXiL4YbJ6n9eeWvVrGyAWGBYstYEJqOrR2KbGcd57R3ZvAkPcH
IdIy+WO9AgMBAAECggEABLbzpG0pmjzhwpSEOnL3trKSO4vHvM1BhzOZ5gH/CqEs
JWdrfGSmHXsTSaethBvoLcuCLYPd8XMw32xOXHDQf9Cc8i4nTcvTN5C5Mt02B5xy
VQLXN8ET0ge19WLQRvpiIxAVBvFc4meNluCeBvmxA0f+cJXbMBqb/Vy+8Vy+FTBs
a3lthG4BVP7/q2SAUbxFQnajSGYHIW7bMKQUThKEPztffiv3pvws2nmSj3A/Ge99
eBRySh9fE2N46QcfAZ7TRMrU+nR6UpH7aBTL0h3T8qTVn/1HdYQyJBtIqhEVFHYZ
q93JkaZJP8Plhvq5gcnrjG1kLBF+w5Uh5d1l5/RqsQKBgQDzgQmCqYIDxWNE1R/d
zL43DOWc0/xA04vVR9NoFr22Yydzv17u34a21LG/TfH3byDSUqd9luKaNy0pykSD
79Vsycg2Bejk5LiZX/5rrX/OGs17Zi2KrQE6DvXdTQOkSaveU7zVKZKyck7mgreW
wXivdVKfqaTrizx68/y94WsY3wKBgQDuJyVAdO4sHJhQOBfp/wCNVfu/BqfQMJpA
/37dVt7HeiMh9hKx0IMY6iVTCXOoFIR9SpH4uiiw8/WkAfcUld4zuE68ymRTaZY7
EgX4i+ltRrXnp1Ac3FSHu972Z2GIFCqcXJ+Qj4aShw1c3bSSuU2pIDYmrAmcfKtK
tu/SAApq4wKBgCBnLm3Nwrhfvur89WWdhj5rH+7zoqC5xeTWzwIN7KbloO1dLPPa
mOGhghmz9Jv5lMOILjOfLX5aE095VA6+jocQfuz5cllrOklmpcOMbfJuTKO8IBlR
FlW0gfE1+2MUTqOiPwGaq6PFZEx2XpnYGwg2M419lK2ndJ/j8eEOqyK/AoGAGDG9
5Rh8Ads91hh8xXb0lWdA1h1U+x+U7DmIp+/lXhqYayDWsV3fk65l8FOrfk3nT9s9
jSlMbP273NeeRGcdVd/JkAB3xMmbS5D/Lkr4gfOHE2u6BdSUed2qPxotnGeAFLaM
N2F9aHFz+BVF/Qn6S85L8g3URCOeO07uekUqycUCgYBsnu7/9m00ZqP9GUyvVzRr
Br3/KmT2lozjcl/DpalWSKCZIW0lYgKYaiWEc165D4vj/ZBhHO7OPGeT2jqHk4/W
YdZ72W2776kWb4YTEdmJPNwgZIVFzcSZXzn8TKnCnwbHEilz51GFrMH1ZJNvLK3Q
tylLU8iZnM9E7+/GSVghdQ==
-----END PRIVATE KEY-----"""

  def tlsConfig(
    alpnProtocols: List[String] = List("h2", "http/1.1"),
    alpnPolicy: AlpnPolicy = AlpnPolicy.NegotiateH2Preferred,
    tlsVersions: List[String] = List("TLSv1.3", "TLSv1.2"),
  ): TlsConfig =
    TlsConfig(
      certChain = TlsSource.PemString(Secret(TestCert)),
      privateKey = TlsSource.PemString(Secret(TestKey)),
      alpnProtocols = alpnProtocols,
      alpnPolicy = alpnPolicy,
      tlsVersions = tlsVersions,
    )

  /**
   * Trust-all client context via the extended trust-manager interface, so the
   * JDK never wraps it with identity checks (loopback IP never matches
   * CN=localhost). Test-only relaxation of trust; wire assertions stay exact.
   */
  def trustAllContext(): SSLContext = {
    val trustAll = Array[TrustManager](new X509ExtendedTrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit                    = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit                    = ()
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit    = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String, socket: Socket): Unit    = ()
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String, engine: SSLEngine): Unit = ()
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
    })
    val ctx      = SSLContext.getInstance("TLS")
    ctx.init(null, trustAll, new java.security.SecureRandom())
    ctx
  }

  def newJdkClient(version: java.net.http.HttpClient.Version): java.net.http.HttpClient =
    java.net.http.HttpClient
      .newBuilder()
      .version(version)
      .sslContext(trustAllContext())
      .connectTimeout(java.time.Duration.ofSeconds(5))
      .build()

  def jdkGet(
    client: java.net.http.HttpClient,
    port: Int,
    path: String,
  ): (java.net.http.HttpClient.Version, Int, String) = {
    val request  = java.net.http.HttpRequest
      .newBuilder(java.net.URI.create(s"https://127.0.0.1:$port$path"))
      .timeout(java.time.Duration.ofSeconds(10))
      .GET()
      .build()
    val response =
      client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
    (response.version(), response.statusCode(), response.body())
  }

  /**
   * Raw TLS handshake only (no HTTP framing): returns the ALPN protocol the
   * server negotiated. Throws `SSLException` against a rejecting server.
   */
  def handshakeOnly(port: Int, clientAlpn: Array[String]): String = {
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(5000)
    val sslSocket = trustAllContext().getSocketFactory
      .createSocket(rawSocket, "127.0.0.1", port, true)
      .asInstanceOf[SSLSocket]
    try {
      val params = sslSocket.getSSLParameters
      params.setApplicationProtocols(clientAlpn)
      sslSocket.setSSLParameters(params)
      sslSocket.setUseClientMode(true)
      sslSocket.startHandshake()
      sslSocket.getApplicationProtocol
    } finally {
      try sslSocket.close()
      catch { case _: Throwable => () }
    }
  }

  /**
   * Drains a raw socket to EOF. Returns the bytes received (typically just a
   * TLS close_notify/alert record when the server closed). Throws
   * `SocketTimeoutException` when the server never closes — the failure mode
   * that proves a missing close.
   */
  def drainToEof(rawSocket: Socket): Array[Byte] = {
    val in   = rawSocket.getInputStream
    val out  = new java.io.ByteArrayOutputStream()
    val buf  = new Array[Byte](1024)
    var open = true
    while (open) {
      val n = in.read(buf)
      if (n < 0) open = false else out.write(buf, 0, n)
    }
    out.toByteArray
  }

  /**
   * Raw H1 request over an ALPN-negotiated TLS socket with an explicit client
   * offer. Returns the negotiated protocol, the response status, and the
   * response body. Independent H1 TLS clients that skip ALPN (for example a JDK
   * client pinned to HTTP/1.1) complete no HTTP against a dispatch port by
   * explicit policy, so every H1 proof here negotiates `http/1.1` exactly.
   */
  def rawTlsH1Get(port: Int, alpn: Array[String], path: String): (String, Int, String) = {
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(5000)
    val sslSocket = trustAllContext().getSocketFactory
      .createSocket(rawSocket, "127.0.0.1", port, false)
      .asInstanceOf[SSLSocket]
    try {
      val params     = sslSocket.getSSLParameters
      params.setApplicationProtocols(alpn)
      sslSocket.setSSLParameters(params)
      sslSocket.setUseClientMode(true)
      sslSocket.startHandshake()
      val negotiated = sslSocket.getApplicationProtocol
      val out        = sslSocket.getOutputStream
      out.write(
        s"GET $path HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".getBytes(
          java.nio.charset.StandardCharsets.US_ASCII,
        ),
      )
      out.flush()
      val in         = sslSocket.getInputStream
      val status     = readAsciiLine(in).split(" ", 3)(1).toInt
      var headers    = Map.empty[String, String]
      var line       = readAsciiLine(in)
      while (line.nonEmpty) {
        val colon = line.indexOf(':')
        headers = headers.updated(
          line.substring(0, colon).trim.toLowerCase(java.util.Locale.ROOT),
          line.substring(colon + 1).trim,
        )
        line = readAsciiLine(in)
      }
      val length     = headers.getOrElse("content-length", "0").toInt
      val bodyBytes  = readExact(in, length)
      (negotiated, status, new String(bodyBytes, java.nio.charset.StandardCharsets.UTF_8))
    } finally {
      try sslSocket.close()
      catch { case _: Throwable => () }
      try rawSocket.close()
      catch { case _: Throwable => () }
    }
  }

  private def readAsciiLine(in: java.io.InputStream): String = {
    val buf   = new java.io.ByteArrayOutputStream()
    var done  = false
    while (!done) {
      val b = in.read()
      if (b < 0) throw new java.io.EOFException("EOF inside H1 response line")
      if (b == '\n') done = true
      else buf.write(b)
    }
    val bytes = buf.toByteArray
    val len   = if (bytes.length > 0 && bytes(bytes.length - 1) == '\r') bytes.length - 1 else bytes.length
    new String(bytes, 0, len, java.nio.charset.StandardCharsets.US_ASCII)
  }

  private def readExact(in: java.io.InputStream, length: Int): Array[Byte] = {
    val out    = new Array[Byte](length)
    var offset = 0
    while (offset < length) {
      val n = in.read(out, offset, length - offset)
      if (n < 0) throw new java.io.EOFException(s"EOF after $offset of $length bytes")
      offset += n
    }
    out
  }
}

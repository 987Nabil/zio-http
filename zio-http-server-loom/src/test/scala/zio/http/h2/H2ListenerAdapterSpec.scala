package zio.http.h2

import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue
import javax.net.ssl.{SSLContext, SSLSocket}

import scala.annotation.experimental

import zio._
import zio.blocks.config.Secret
import zio.test.TestAspect.sequential
import zio.test._

import zio.http.{TlsConfig, TlsSource}

/**
 * Todo 4 RED: the H2 listener adapter.
 *
 * Pins that the old `zio.http.h2.TcpListener` construction keeps working after
 * the protocol-independent extraction: the adapter delegates accept/TLS
 * ownership to the generic listener, preserves the
 * `(InputStream, OutputStream, PeerInfo)` handler shape, keeps
 * `createSslContext` available, and still rejects non-h2 ALPN at the TLS layer
 * under the default strict policy.
 */
@experimental
object H2ListenerAdapterSpec extends ZIOSpecDefault {

  private val TestCert =
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

  private val TestKey =
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

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("H2ListenerAdapterSpec")(
      test("old TcpListener construction still serves cleartext with PeerInfo") {
        val peers = new LinkedBlockingQueue[String]()
        ZIO.attemptBlocking {
          val listener = new TcpListener(
            "127.0.0.1",
            0,
            None,
            (in, out, peer) => {
              peers.offer(peer.address)
              val buf = new Array[Byte](1024)
              val n   = in.read(buf)
              if (n > 0) {
                out.write(buf, 0, n)
                out.flush()
              }
            },
          )
          val bound    = listener.start()
          try {
            val socket = new Socket("127.0.0.1", bound.port)
            socket.setSoTimeout(5000)
            try {
              socket.getOutputStream.write("adapter-ping".getBytes(StandardCharsets.UTF_8))
              socket.shutdownOutput()
              val received = readAll(socket)
              (new String(received, StandardCharsets.UTF_8), peers.poll(5L, java.util.concurrent.TimeUnit.SECONDS))
            } finally socket.close()
          } finally bound.close()
        }.map { case (text, peerAddress) =>
          assertTrue(text == "adapter-ping", peerAddress == "127.0.0.1")
        }
      },
      test("adapter delegates TLS context creation and keeps StrictH2 rejection") {
        val tlsCfg = TlsConfig(
          certChain = TlsSource.PemString(Secret(TestCert)),
          privateKey = TlsSource.PemString(Secret(TestKey)),
        )
        for {
          ctx  <- ZIO.attempt(TcpListener.createSslContext(tlsCfg))
          exit <- ZIO.attemptBlocking {
            val listener = new TcpListener(
              "127.0.0.1",
              0,
              Some(tlsCfg),
              (in, out, _) => {
                out.write(1)
                out.flush()
              },
            )
            val bound    = listener.start()
            try handshakeOnly(bound.port, Array("http/1.1"))
            finally bound.close()
          }.exit
        } yield {
          val rejected = exit match {
            case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[javax.net.ssl.SSLException])
            case _                   => false
          }
          assertTrue(ctx != null, exit.isFailure, rejected)
        }
      },
    ) @@ sequential

  private def readAll(socket: Socket): Array[Byte] = {
    val in   = socket.getInputStream
    val out  = new java.io.ByteArrayOutputStream()
    val buf  = new Array[Byte](1024)
    var open = true
    while (open) {
      val n = in.read(buf)
      if (n < 0) open = false else out.write(buf, 0, n)
    }
    out.toByteArray
  }

  private def handshakeOnly(port: Int, clientAlpn: Array[String]): String = {
    val trustAll  = Array[javax.net.ssl.TrustManager](new javax.net.ssl.X509TrustManager {
      override def checkClientTrusted(c: Array[java.security.cert.X509Certificate], a: String): Unit = ()
      override def checkServerTrusted(c: Array[java.security.cert.X509Certificate], a: String): Unit = ()
      override def getAcceptedIssuers: Array[java.security.cert.X509Certificate]                     = Array.empty
    })
    val ctx       = SSLContext.getInstance("TLS")
    ctx.init(null, trustAll, new java.security.SecureRandom())
    val rawSocket = new Socket("127.0.0.1", port)
    rawSocket.setSoTimeout(5000)
    val sslSocket = ctx.getSocketFactory
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
}

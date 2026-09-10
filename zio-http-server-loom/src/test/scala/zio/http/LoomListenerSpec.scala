package zio.http

import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.{SSLContext, SSLSocket, TrustManager, X509TrustManager}

import scala.annotation.experimental
import scala.util.control.NonFatal

import zio._
import zio.blocks.config.Secret
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 4 RED: protocol-independent Loom listener contract.
 *
 * Pins what the generic listener extracted from `zio.http.h2.TcpListener` must
 * do, without any framing or routing:
 *   - ephemeral loopback cleartext round trip with peer metadata and no ALPN;
 *   - TLS loopback handshake surfacing the negotiated ALPN and secure flag;
 *   - configured ALPN lists/policies surfaced verbatim (http/1.1 and empty);
 *   - bind conflict fails fast and leaves the first listener serving;
 *   - TLS handshake failure never reaches the handler and cleans up;
 *   - close during a blocked accept terminates promptly and idempotently;
 *   - connection work runs on virtual threads with no thread residue.
 */
@experimental
object LoomListenerSpec extends ZIOSpecDefault {

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

  /** Observation captured by test handlers: the connection plus the carrier. */
  private final case class Observation(
    connection: AcceptedConnection,
    thread: Thread,
    threadName: String,
    isVirtual: Boolean,
  )

  /** Echo handler that records every accepted connection. */
  private final class EchoCapture {
    val seen: LinkedBlockingQueue[Observation] = new LinkedBlockingQueue[Observation]()

    val handler: AcceptedConnection => Unit = conn => {
      val self = Thread.currentThread()
      seen.offer(Observation(conn, self, self.getName, self.isVirtual))
      val in   = conn.input
      val out  = conn.output
      val buf  = new Array[Byte](1024)
      var open = true
      try {
        while (open) {
          val n = in.read(buf)
          if (n < 0) open = false
          else {
            out.write(buf, 0, n)
            out.flush()
          }
        }
      } catch {
        // Listener close races a blocked read when the test probes
        // cancellation; the residue assertions own the verdict, not the log.
        case NonFatal(_) => ()
      }
    }
  }

  /** Capture-only handler: records the connection and returns immediately. */
  private final class MetadataCapture {
    val seen: LinkedBlockingQueue[Observation] = new LinkedBlockingQueue[Observation]()

    val handler: AcceptedConnection => Unit = conn => {
      val self = Thread.currentThread()
      seen.offer(Observation(conn, self, self.getName, self.isVirtual))
    }
  }

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("LoomListenerSpec")(
      test("cleartext loopback round trip surfaces peer metadata with no ALPN") {
        val capture = new EchoCapture()
        withListener(new LoomListener("127.0.0.1", 0, None, capture.handler)) { bound =>
          ZIO.attemptBlocking {
            val socket = new Socket("127.0.0.1", bound.port)
            socket.setSoTimeout(5000)
            try {
              val payload  = "loom-listener-ping".getBytes(StandardCharsets.UTF_8)
              socket.getOutputStream.write(payload)
              socket.shutdownOutput()
              val received = readAll(socket)
              val obs      = pollObservation(capture.seen)
              assertTrue(
                new String(received, StandardCharsets.UTF_8) == "loom-listener-ping",
                obs.isDefined,
                obs.exists(_.connection.negotiatedAlpn.isEmpty),
                obs.exists(_.connection.secure == false),
                obs.exists(_.connection.peer.address == "127.0.0.1"),
                obs.exists(_.isVirtual),
              )
            } finally socket.close()
          }
        }
      },
      test("TLS loopback handshake surfaces negotiated h2 ALPN") {
        val capture = new MetadataCapture()
        val tlsCfg  = TlsConfig(
          certChain = TlsSource.PemString(Secret(TestCert)),
          privateKey = TlsSource.PemString(Secret(TestKey)),
        )
        withListener(new LoomListener("127.0.0.1", 0, Some(tlsCfg), capture.handler)) { bound =>
          ZIO.attemptBlocking {
            val negotiated = handshakeOnly(bound.port, Array("h2"))
            val obs        = pollObservation(capture.seen)
            assertTrue(
              negotiated == "h2",
              obs.isDefined,
              obs.exists(_.connection.negotiatedAlpn == Some("h2")),
              obs.exists(_.connection.secure),
              obs.exists(_.connection.peer.address == "127.0.0.1"),
              obs.exists(_.isVirtual),
            )
          }
        }
      },
      test("NegotiateH2Preferred surfaces http/1.1 and empty ALPN without framing") {
        val capture = new MetadataCapture()
        val tlsCfg  = TlsConfig(
          certChain = TlsSource.PemString(Secret(TestCert)),
          privateKey = TlsSource.PemString(Secret(TestKey)),
          alpnProtocols = List("h2", "http/1.1"),
          alpnPolicy = AlpnPolicy.NegotiateH2Preferred,
        )
        withListener(new LoomListener("127.0.0.1", 0, Some(tlsCfg), capture.handler)) { bound =>
          ZIO.attemptBlocking {
            val negotiatedHttp1 = handshakeOnly(bound.port, Array("http/1.1"))
            val first           = pollObservation(capture.seen)
            val negotiatedEmpty = handshakeOnly(bound.port, Array.empty)
            val second          = pollObservation(capture.seen)
            assertTrue(
              negotiatedHttp1 == "http/1.1",
              first.exists(_.connection.negotiatedAlpn == Some("http/1.1")),
              first.exists(_.connection.secure),
              negotiatedEmpty == "",
              second.exists(_.connection.negotiatedAlpn.isEmpty),
              second.exists(_.connection.secure),
            )
          }
        }
      },
      test("bind conflict fails fast and leaves the first listener serving") {
        val capture = new EchoCapture()
        ZIO
          .acquireRelease(ZIO.attempt(new LoomListener("127.0.0.1", 0, None, capture.handler).start()))(bound =>
            ZIO.succeed(bound.close()),
          )
          .flatMap { first =>
            val port = first.port
            for {
              second <- ZIO.attempt(new LoomListener("127.0.0.1", port, None, capture.handler).start()).exit
              alive = first.isRunning()
              text <- ZIO.attemptBlocking {
                val socket = new Socket("127.0.0.1", port)
                socket.setSoTimeout(5000)
                try {
                  socket.getOutputStream.write("still-here".getBytes(StandardCharsets.UTF_8))
                  socket.shutdownOutput()
                  new String(readAll(socket), StandardCharsets.UTF_8)
                } finally socket.close()
              }
            } yield assertTrue(second.isFailure, alive, text == "still-here")
          }
      },
      test("TLS handshake failure never reaches the handler and cleans up") {
        val capture = new MetadataCapture()
        val tlsCfg  = TlsConfig(
          certChain = TlsSource.PemString(Secret(TestCert)),
          privateKey = TlsSource.PemString(Secret(TestKey)),
        )
        withListener(new LoomListener("127.0.0.1", 0, Some(tlsCfg), capture.handler)) { bound =>
          ZIO.attemptBlocking {
            val probe      = new Socket("127.0.0.1", bound.port)
            probe.setSoTimeout(5000)
            try {
              probe.getOutputStream.write("NOT-TLS-PROBE\r\n\r\n".getBytes(StandardCharsets.UTF_8))
              probe.getOutputStream.flush()
            } finally probe.close()
            // Give the failed handshake time to land any stray handler
            // invocation, then prove none arrived: the handler queue itself is
            // the residue detector (no global thread enumeration, which omits
            // unmounted virtual threads on this JDK).
            Thread.sleep(1000L)
            val noStray    = capture.seen.isEmpty
            // The listener survives the probe: a real handshake still works.
            val negotiated = handshakeOnly(bound.port, Array("h2"))
            val obs        = pollObservation(capture.seen)
            assertTrue(
              noStray,
              bound.isRunning(),
              negotiated == "h2",
              obs.exists(_.connection.negotiatedAlpn == Some("h2")),
            )
          }
        }
      },
      test("close during blocked accept terminates promptly and idempotently") {
        ZIO.attemptBlocking {
          val bound     = new LoomListener("127.0.0.1", 0, None, _ => ()).start()
          assertTrue(bound.isRunning())
          val startMs   = java.lang.System.nanoTime()
          bound.close()
          bound.close()
          val elapsedMs = (java.lang.System.nanoTime() - startMs) / 1000000L
          assertTrue(!bound.isRunning(), elapsedMs < 10000L)
        }
      },
      test("connection threads are virtual and leave no residue") {
        val capture = new EchoCapture()
        withListener(new LoomListener("127.0.0.1", 0, None, capture.handler)) { bound =>
          ZIO.attemptBlocking {
            val socket = new Socket("127.0.0.1", bound.port)
            socket.setSoTimeout(5000)
            try {
              socket.getOutputStream.write("hold-open".getBytes(StandardCharsets.UTF_8))
              socket.getOutputStream.flush()
              val obs      = pollObservation(capture.seen)
              val observed = obs.isDefined
              val virtual  = obs.exists(_.isVirtual)
              val named    = obs.exists(_.threadName.startsWith("zio-http-conn-"))
              // Cancellation probe: closing the listener with a connection
              // still open must terminate the connection thread promptly; the
              // tracked carrier thread itself is the residue detector.
              bound.close()
              val drained  = obs.exists(t => pollFor(5000L)(!t.thread.isAlive))
              val stopped  = !bound.isRunning()
              // The binding is gone: fresh connects are refused.
              val refused  =
                try {
                  val probe = new Socket("127.0.0.1", bound.port)
                  try false
                  finally probe.close()
                } catch {
                  case _: java.io.IOException => true
                }
              assertTrue(observed, virtual, named, drained, stopped, refused)
            } finally socket.close()
          }
        }
      },
    ) @@ sequential

  private def withListener[R](listener: LoomListener)(
    use: LoomBoundListener => ZIO[R with Scope, Throwable, TestResult],
  ): ZIO[R with Scope, Throwable, TestResult] =
    ZIO
      .acquireRelease(ZIO.attempt(listener.start()))(bound => ZIO.succeed(bound.close()))
      .flatMap(use)

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

  private def pollObservation(queue: LinkedBlockingQueue[Observation]): Option[Observation] = {
    val deadline = java.lang.System.currentTimeMillis() + 5000L
    var next     = Option(queue.poll())
    while (next.isEmpty && java.lang.System.currentTimeMillis() < deadline) {
      Thread.sleep(50L)
      next = Option(queue.poll())
    }
    next
  }

  /** Test-only bounded wait; the listener implementation itself never polls. */
  private def pollFor(timeoutMs: Long)(cond: => Boolean): Boolean = {
    val deadline = java.lang.System.currentTimeMillis() + timeoutMs
    var ok       = cond
    while (!ok && java.lang.System.currentTimeMillis() < deadline) {
      Thread.sleep(50L)
      ok = cond
    }
    ok
  }

  private def trustAllContext(): SSLContext = {
    val trustAll = Array[TrustManager](new X509TrustManager {
      override def checkClientTrusted(chain: Array[java.security.cert.X509Certificate], authType: String): Unit = ()
      override def checkServerTrusted(chain: Array[java.security.cert.X509Certificate], authType: String): Unit = ()
      override def getAcceptedIssuers: Array[java.security.cert.X509Certificate] = Array.empty
    })
    val ctx      = SSLContext.getInstance("TLS")
    ctx.init(null, trustAll, new java.security.SecureRandom())
    ctx
  }

  private def handshakeOnly(port: Int, clientAlpn: Array[String]): String = {
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
}

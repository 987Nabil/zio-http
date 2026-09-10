package zio.http

import scala.annotation.experimental

import zio._
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 12 RED: pure cleartext preface-selection contract.
 *
 * Baseline characterization: cleartext connectors serve exactly one protocol —
 * `H1Transport` speaks strict H1, `H2Transport` speaks H2C and its
 * `H2Connection` fails any non-preface bytes with a connection error. No shared
 * cleartext endpoint exists: H1 bytes sent to an H2C port die in the H2 parser,
 * and an H2 preface sent to an H1 port dies in the strict H1 codec. There is no
 * replay buffer, no prefix timeout, and no half-preface admission cap.
 *
 * This spec pins the per-connection decision before any socket exists:
 *   - the exact 24-byte client connection preface (RFC 9113 section 3.1)
 *     identifies H2C only when complete;
 *   - any diverged opening bytes select H1 with exactly-once replay;
 *   - a near-preface (long exact-prefix match that then diverges) rejects
 *     rather than downgrading to H1;
 *   - selection honors the configured [[ProtocolSet]] (missing members fail
 *     typed via `Negotiation.selectForPreface`, never silently downgrade);
 *   - no h2c Upgrade semantics exist anywhere on this path.
 */
@experimental
object CleartextPrefaceDispatchSpec extends ZIOSpecDefault {

  private val PrefaceText = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("CleartextPrefaceDispatchSpec")(
      test("exact 24-byte preface identifies H2C") {
        ZIO.attempt {
          val bytes = PrefaceText.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          assertTrue(
            bytes.length == CleartextPrefaceDispatch.PrefaceLength,
            CleartextPrefaceDispatch.isH2Preface(bytes),
            CleartextPrefaceDispatch.classify(bytes) == CleartextPrefaceDispatch.Decision.H2,
          )
        }
      },
      test("H1 opening bytes select H1 for replay") {
        ZIO.attempt {
          val bytes = "GET /ok HTTP/1.1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          assertTrue(
            !CleartextPrefaceDispatch.isH2Preface(bytes),
            CleartextPrefaceDispatch.classify(bytes) == CleartextPrefaceDispatch.Decision.H1,
          )
        }
      },
      test("every strict prefix of the preface is still possible H2") {
        ZIO.attempt {
          val full   = PrefaceText.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          var index  = 0
          var failed = false
          while (index < full.length && !failed) {
            val prefix = java.util.Arrays.copyOf(full, index)
            if (CleartextPrefaceDispatch.classify(prefix) != CleartextPrefaceDispatch.Decision.NeedMore)
              failed = true
            index += 1
          }
          assertTrue(!failed)
        }
      },
      test("near-preface divergence rejects instead of downgrading") {
        ZIO.attempt {
          // A typo inside the 24-byte window rejects:
          val typo = "PRI * HTTP/2.0\r\n\r\nXM\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          assertTrue(
            CleartextPrefaceDispatch.classify(typo) == CleartextPrefaceDispatch.Decision.Reject,
            CleartextPrefaceDispatch.isNearPreface(typo),
            !CleartextPrefaceDispatch.isNearPreface(
              "GET /ok HTTP/1.1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
            ),
          )
        }
      },
      test("selection honors the configured protocol set") {
        ZIO.attempt {
          val preface = PrefaceText.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          val h1bytes = "GET /ok HTTP/1.1\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
          assertTrue(
            CleartextPrefaceDispatch.select(ProtocolSet.h1h2c, preface) == Right(AppProtocol.H2C),
            CleartextPrefaceDispatch.select(ProtocolSet.h1h2c, h1bytes) == Right(AppProtocol.Http1),
            CleartextPrefaceDispatch.select(ProtocolSet.h1Only, preface).isLeft,
            CleartextPrefaceDispatch.select(ProtocolSet.h2cOnly, h1bytes).isLeft,
          )
        }
      },
      test("empty input needs more bytes, never a silent decision") {
        ZIO.attempt {
          assertTrue(
            CleartextPrefaceDispatch.classify(Array.emptyByteArray) == CleartextPrefaceDispatch.Decision.NeedMore,
          )
        }
      },
      test("connector literals stay in sync with the documented defaults") {
        ZIO.attempt {
          val connector = Connector(bind = BindAddress.localhost(0))
          assertTrue(
            connector.prefaceTimeoutMs == Connector.DefaultPrefaceTimeoutMs,
            connector.maxHalfPrefaceConnections == Connector.DefaultMaxHalfPrefaceConnections,
          )
        }
      },
    ) @@ sequential
}

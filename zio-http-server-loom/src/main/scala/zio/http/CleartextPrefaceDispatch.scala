package zio.http

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.SequenceInputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.experimental

import scala.util.control.NonFatal

/**
 * Todo 12: pure cleartext preface selection plus the bounded sniff that
 * executes it.
 *
 * Decides only — the sniff never touches sockets beyond the accepted byte
 * stream. An exact 24-byte client connection preface (RFC 9113 section 3.1,
 * `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`) selects `H2C`; any diverged opening bytes
 * select `H1` with exactly-once replay; a near-preface (a long exact-prefix
 * match that then diverges) rejects rather than downgrading to H1, so a
 * mistyped preface can never be reinterpreted as an H1 request. Selection
 * honors the configured [[ProtocolSet]] through
 * [[Negotiation.selectForPreface]]: a missing member fails typed, never
 * silently downgrades.
 *
 * H2 framing validity past the preface (the client SETTINGS frame) is enforced
 * by `H2Connection` itself on the replayed stream: an exact preface followed by
 * garbage closes the connection without H1 fallback. Dispatch therefore never
 * forks H2 frame logic and the replay buffer stays exactly 24 bytes.
 *
 * No h2c Upgrade semantics exist on this path: `Upgrade: h2c` request headers
 * are ordinary H1 headers, ignored by the strict H1 engine.
 */
@experimental
object CleartextPrefaceDispatch {

  /** Length of the RFC 9113 client connection preface in bytes. */
  val PrefaceLength: Int = 24

  /**
   * Minimum exact-prefix length that marks diverged bytes as a malformed
   * near-preface instead of H1. No valid H1 request line shares a 4-byte prefix
   * with the preface (`PRI `): every registered H1 method (`GET `, `POST`,
   * `PUT `, `PATCH`, `PROPFIND`, ...) diverges within the first three bytes, so
   * a 4-byte match followed by divergence is a mistyped preface, never H1.
   */
  val NearPrefixThreshold: Int = 4

  private val PrefaceBytes: Array[Byte] =
    "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII)

  /**
   * Exact match of the full 24-byte preface (longer inputs compare the head).
   */
  def isH2Preface(bytes: Array[Byte]): Boolean = {
    if (bytes.length < PrefaceLength) false
    else {
      var index   = 0
      var matched = true
      while (index < PrefaceLength && matched) {
        if (bytes(index) != PrefaceBytes(index)) matched = false
        index += 1
      }
      matched
    }
  }

  /** True when `bytes` are an exact prefix of the preface (possibly empty). */
  def isPossiblePrefix(bytes: Array[Byte], length: Int): Boolean = {
    var index   = 0
    var matched = true
    while (index < length && matched) {
      if (bytes(index) != PrefaceBytes(index)) matched = false
      index += 1
    }
    matched
  }

  /**
   * True when the buffered bytes share a [[NearPrefixThreshold]]-or-longer head
   * with the preface but are not the exact preface: a mistyped preface that
   * must reject rather than downgrade. Short divergences (ordinary H1 bytes)
   * are not near.
   */
  def isNearPreface(bytes: Array[Byte]): Boolean =
    !isH2Preface(bytes) && commonPrefixLength(bytes) >= NearPrefixThreshold

  private def commonPrefixLength(bytes: Array[Byte]): Int = {
    val bound   = Math.min(bytes.length, PrefaceLength)
    var index   = 0
    var matched = true
    while (index < bound && matched) {
      if (bytes(index) != PrefaceBytes(index)) matched = false
      else index += 1
    }
    index
  }

  /** Per-connection classification of the bytes buffered so far. */
  sealed trait Decision
  object Decision {
    case object H2       extends Decision
    case object H1       extends Decision
    case object Reject   extends Decision
    case object NeedMore extends Decision
  }

  /**
   * Classifies buffered opening bytes: a complete exact preface is H2, a
   * complete diverged window is H1 unless near (then Reject), an exact prefix
   * of the preface needs more bytes, and an early divergence decides H1 (or
   * Reject for a long-match-then-diverge) without waiting for the full window.
   */
  def classify(bytes: Array[Byte]): Decision =
    if (bytes.length >= PrefaceLength) {
      if (isH2Preface(bytes)) Decision.H2
      else if (isNearPreface(bytes)) Decision.Reject
      else Decision.H1
    } else if (isPossiblePrefix(bytes, bytes.length)) Decision.NeedMore
    else if (isNearPreface(bytes)) Decision.Reject
    else Decision.H1

  /**
   * Selects the protocol for decided bytes against the configured set. H2 maps
   * through `selectForPreface(true)`, H1 through `selectForPreface(false)`; a
   * `NeedMore` (truncated) or `Reject` (malformed near-preface) input fails
   * with `NoProtocolForPreface(isH2Preface = true)` — bytes that claimed H2
   * framing but never completed it — instead of downgrading silently.
   */
  def select(set: ProtocolSet, bytes: Array[Byte]): Either[ConnectorFailure, AppProtocol] =
    classify(bytes) match {
      case Decision.H2 => Negotiation.selectForPreface(set, isH2Preface = true)
      case Decision.H1 => Negotiation.selectForPreface(set, isH2Preface = false)
      case _           => Left(ConnectorFailure.NoProtocolForPreface(isH2Preface = true))
    }

  /** Sniff outcome: replay stream for exactly-once handoff, or rejection. */
  sealed trait SniffOutcome
  object SniffOutcome {
    final case class ToH2(replay: InputStream) extends SniffOutcome
    final case class ToH1(replay: InputStream) extends SniffOutcome
    case object Reject                         extends SniffOutcome
  }

  /**
   * Reads up to 24 opening bytes with a one-shot deadline and decides.
   *
   * Returns `ToH2` only on the complete exact preface (the 24 bytes replay
   * exactly once ahead of the live stream; H2 enforces SETTINGS framing and
   * closes garbage without H1 fallback), `ToH1` on early divergence (buffered
   * bytes replay exactly once ahead of the live stream), and `Reject` on
   * near-preface divergence, EOF, timeout, or read failure — always closing
   * without serving.
   *
   * The deadline uses a one-shot virtual-thread guard that closes the stream on
   * expiry (the same pattern as `H1Transport` request guards): no polling loop,
   * no monitor `synchronized`. Non-positive `timeoutMs` disables.
   */
  def sniff(input: InputStream, timeoutMs: Long): SniffOutcome = {
    val cancel = armDeadline(timeoutMs, () => closeQuietly(input))
    try {
      val buf                = new Array[Byte](PrefaceLength)
      var filled             = 0
      var done: SniffOutcome = null
      while (done == null) {
        val n =
          try input.read(buf, filled, PrefaceLength - filled)
          catch {
            case NonFatal(_) => -2
          }
        if (n < 0) {
          // EOF (-1) or a read failure (-2, including our own deadline
          // close): the preface never completed while still possible — reject
          // without serving. Divergence would already have decided below.
          done = SniffOutcome.Reject
        } else if (n > 0) {
          filled += n
          classify(java.util.Arrays.copyOf(buf, filled)) match {
            case Decision.H2       =>
              done = SniffOutcome.ToH2(replay(buf, filled, input))
            case Decision.H1       =>
              done = SniffOutcome.ToH1(replay(buf, filled, input))
            case Decision.Reject   =>
              done = SniffOutcome.Reject
            case Decision.NeedMore => ()
          }
        }
      }
      done
    } finally cancel()
  }

  private def replay(buf: Array[Byte], length: Int, rest: InputStream): InputStream =
    new SequenceInputStream(new ByteArrayInputStream(java.util.Arrays.copyOf(buf, length)), rest)

  /**
   * One-shot virtual-thread deadline: sleeps once, fires once, dies on cancel.
   * Never a poll loop; healthy sniffs pay one interrupt. Non-positive disables.
   */
  private def armDeadline(timeoutMs: Long, onExpire: () => Unit): () => Unit =
    if (timeoutMs <= 0L) () => ()
    else {
      val live   = new AtomicBoolean(true)
      val thread = Thread
        .ofVirtual()
        .name("zio-http-cleartext-preface-deadline")
        .start(new Runnable {
          def run(): Unit =
            try {
              Thread.sleep(timeoutMs)
              if (live.compareAndSet(true, false)) onExpire()
            } catch {
              case _: InterruptedException => ()
            }
        })
      () => if (live.compareAndSet(true, false)) thread.interrupt()
    }

  private def closeQuietly(resource: AutoCloseable): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case NonFatal(_) => ()
      }
    }
}

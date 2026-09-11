package zio.http

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

/**
 * Protocol labels shared by every engine's telemetry.
 *
 * Uses the same vocabulary as the H2 transport's wire name (`h2c`/`h2`) so
 * existing dashboards keep working; the H1 engine adopts it (`h1`) instead of
 * inventing a second naming scheme.
 */
object ProtocolLabel {

  /** Loom HTTP/1.1 engine. */
  val H1: String = "h1"

  /** Cleartext HTTP/2. */
  val H2C: String = "h2c"

  /** HTTP/2 over TLS. */
  val H2: String = "h2"
}

/**
 * Timeout dimension for [[ServerDiagnostic.RequestTimeout]].
 *
 * Values render as `"request"`, `"header"`, `"body"`, `"idle"` (stable log
 * vocabulary).
 */
sealed abstract class TimeoutKind(val value: String) {
  override def toString: String = value
}

object TimeoutKind {
  case object Request extends TimeoutKind("request")
  case object Header  extends TimeoutKind("header")
  case object Body    extends TimeoutKind("body")
  case object Idle    extends TimeoutKind("idle")
}

/**
 * Stream-reset cause for [[ServerDiagnostic.StreamReset]].
 *
 * H2 causes reuse the RFC 9113 error-code names; `Aborted` marks a response
 * that started and then failed mid-stream; `Closed` marks a peer-gone reset
 * with no code on the wire. Values render as their wire name (stable log
 * vocabulary).
 */
sealed abstract class StreamResetReason(val value: String) {
  override def toString: String = value
}

object StreamResetReason {
  case object Cancel           extends StreamResetReason("CANCEL")
  case object FlowControlError extends StreamResetReason("FLOW_CONTROL_ERROR")
  case object ProtocolError    extends StreamResetReason("PROTOCOL_ERROR")
  case object Refused          extends StreamResetReason("REFUSED_STREAM")
  case object Aborted          extends StreamResetReason("aborted")
  case object Closed           extends StreamResetReason("closed")
}

/**
 * One protocol-neutral server diagnostic event.
 *
 * Every event carries the serving `protocol` ([[ProtocolLabel]]) and a
 * `connector` label ([[ServerTelemetry.connectorLabel]]) so shared
 * metrics/tracing/access sinks can slice by engine and binding. Wire failures
 * stay protocol-specific: the typed detail (`errorType`, [[TimeoutKind]],
 * [[StreamResetReason]], [[ConnectorFailure]]) names the failure class, never
 * its payload.
 *
 * Redaction by construction: cases carry method, path, status codes, error
 * class names and counters only. There is no header-map, body, secret, or raw
 * wire-bytes field anywhere in this vocabulary, so no sink — however buggy —
 * can leak them. `path` is logged verbatim, matching standard access-log
 * behavior (see [[AccessLog]]).
 */
sealed trait ServerDiagnostic {

  /** Serving protocol (`h1`, `h2c`, `h2`). */
  def protocol: String

  /** Stable connector label; never carries secrets (see `connectorLabel`). */
  def connector: String
}

object ServerDiagnostic {

  /** One request served to completion (any status, including handler 500s). */
  final case class RequestCompleted(
    protocol: String,
    connector: String,
    method: String,
    path: String,
    status: Int,
    durationMs: Long,
  ) extends ServerDiagnostic

  /**
   * One request rejected before (or without) a normal dispatch: strict-parser
   * rejections (H1 `400`, body-cap `413`, unknown method `501`), over-cap or
   * length-mismatched H2 bodies, and unexpected dispatch failures (`500`).
   * `errorType` is the wire error's class simple name (or a fixed token such as
   * `UnknownMethod`); `status` is the terminal HTTP status when one is sent
   * (`None` for H2 stream resets, which carry no HTTP status).
   */
  final case class ParseRejected(
    protocol: String,
    connector: String,
    errorType: String,
    status: Option[Int],
  ) extends ServerDiagnostic

  /** A request/body/header/idle deadline expired. */
  final case class RequestTimeout(
    protocol: String,
    connector: String,
    timeoutKind: TimeoutKind,
    errorType: String,
  ) extends ServerDiagnostic

  /**
   * A stream (H2) or in-flight response turnover reset; `streamId` is H2-only.
   */
  final case class StreamReset(
    protocol: String,
    connector: String,
    reason: StreamResetReason,
    streamId: Option[Int],
  ) extends ServerDiagnostic

  /** A connection was accepted and handed to an engine. */
  final case class ConnectionOpened(
    protocol: String,
    connector: String,
  ) extends ServerDiagnostic

  /**
   * A connection closed. `drained` is true only when the close follows a
   * graceful drain; force-closes report false — even when a drain was also
   * requested, because the force won the race.
   */
  final case class ConnectionClosed(
    protocol: String,
    connector: String,
    drained: Boolean,
  ) extends ServerDiagnostic

  /** A bind attempt failed; `errorType` is the exception class simple name. */
  final case class BindFailure(
    connector: String,
    errorType: String,
  ) extends ServerDiagnostic {
    def protocol: String = ""
  }

  /**
   * A per-connection protocol selection failed with its typed
   * [[ConnectorFailure]] (unknown ALPN, missing preface member, unadvertised
   * H3, ...). Callers match on `failure` — never on rendered text.
   */
  final case class NegotiationFailure(
    connector: String,
    failure: ConnectorFailure,
  ) extends ServerDiagnostic {
    def protocol: String = ""
  }
}

/**
 * Receiver for [[ServerDiagnostic]]s.
 *
 * Implementations run on the server I/O path, so `record` should be fast and
 * non-blocking. Engines take one as a constructor parameter defaulting to the
 * process-global bridge, so production keeps emitting the shared
 * metrics/tracing/logs while tests inject [[ServerTelemetry.InMemory]] for
 * deterministic assertions.
 */
trait ServerTelemetry {

  /** Receives one diagnostic event. */
  def record(diagnostic: ServerDiagnostic): Unit
}

object ServerTelemetry {

  /** Discards every diagnostic. */
  val noop: ServerTelemetry =
    new ServerTelemetry {
      def record(diagnostic: ServerDiagnostic): Unit = ()
    }

  /**
   * In-memory capture sink for specs: appends every diagnostic in arrival
   * order. Backed by a concurrent queue because engines record from connection
   * threads, not the test thread.
   */
  final class InMemory extends ServerTelemetry {
    private val queue = new ConcurrentLinkedQueue[ServerDiagnostic]()

    def record(diagnostic: ServerDiagnostic): Unit =
      queue.add(diagnostic)

    /** Snapshot of recorded diagnostics in arrival order. */
    def records: List[ServerDiagnostic] =
      queue.asScala.toList

    /** Drops all recorded diagnostics. */
    def clear(): Unit =
      queue.clear()
  }

  /**
   * Stable connector label for telemetry: `tcp/<host>:<port>/<family>` or
   * `unix/<path>`, where `<family>` is the legacy application protocol (`h2c`,
   * `h2`, `h3`).
   *
   * Only the bind address and the protocol family cross into telemetry. TLS
   * material (key/certificate [[zio.blocks.config.Secret]]s), timeouts, and
   * proxy config never do — a `Connector` carrying secrets labels exactly like
   * one without them.
   */
  def connectorLabel(connector: Connector): String = {
    val family = connector.protocol match {
      case Protocol.H2C(_)      => ProtocolLabel.H2C
      case Protocol.H2(_, _)    => ProtocolLabel.H2
      case Protocol.H3(_, _, _) => "h3"
    }
    connector.bind match {
      case BindAddress.Tcp(host, port) => "tcp/" + host + ":" + port + "/" + family
      case BindAddress.Unix(path)      => "unix/" + path.toString
    }
  }

  /**
   * Sanitizes an attacker-influenced protocol token (e.g. an offered ALPN id)
   * for safe logging: keeps alphanumerics plus `-._~/`, truncates to 64
   * characters, and yields `-` when nothing survives. Mirrors
   * [[AccessLog.sanitizeRequestId]].
   */
  def sanitizeToken(raw: String): String = {
    val kept = new StringBuilder()
    var i    = 0
    while (i < raw.length && kept.length < 64) {
      val c = raw.charAt(i)
      if (
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
        c == '-' || c == '.' || c == '_' || c == '~' || c == '/'
      ) kept.append(c)
      i += 1
    }
    if (kept.isEmpty) "-" else kept.toString
  }
}

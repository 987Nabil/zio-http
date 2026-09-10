package zio.http

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.experimental

import zio.blocks.telemetry.{AttributeValue, ConsoleLogRecordProcessor, LoggerProvider, SpanKind, metric, trace}

import zio.http.ServerDiagnostic._

/**
 * Process-global [[ServerTelemetry]] bridge (Todo 9): maps every
 * [[ServerDiagnostic]] onto the shared OpenTelemetry surfaces — the
 * `http.requests.total` / `http.connections.active` instruments the H2
 * transport has always emitted (now with `protocol`/`connector` labels on every
 * engine), per-signal counters for rejections/timeouts/resets/closes, one
 * `http.request` server span per request-scoped event, and one structured log
 * per event.
 *
 * Engines default to this bridge so production behavior is preserved with no
 * wiring; specs inject [[ServerTelemetry.InMemory]] instead. Only metadata
 * crosses into metrics/spans/logs: method, path (verbatim, access-log
 * convention), status codes, error class names, and fixed-vocabulary labels.
 * Failure details that could carry attacker bytes (e.g. an offered ALPN id)
 * pass through [[ServerTelemetry.sanitizeToken]] first.
 */
@experimental
object LoomServerTelemetry {

  /** The process-global bridge engines use by default. */
  val global: ServerTelemetry = new Bridge()

  private final class Bridge extends ServerTelemetry {
    private val requestsTotal            = metric.counter("http.requests.total")
    private val rejectedTotal            = metric.counter("http.requests.rejected.total")
    private val timeoutTotal             = metric.counter("http.requests.timeout.total")
    private val resetTotal               = metric.counter("http.streams.reset.total")
    private val closedTotal              = metric.counter("http.connections.closed.total")
    private val bindFailuresTotal        = metric.counter("http.bind.failures.total")
    private val negotiationFailuresTotal = metric.counter("http.negotiation.failures.total")
    private val activeConnections        = metric.upDownCounter("http.connections.active")
    private val activeCount              = new AtomicLong(0L)
    private val logger                   =
      LoggerProvider.builder
        .addLogRecordProcessor(new ConsoleLogRecordProcessor)
        .build()
        .get("zio.http.ServerTelemetry")

    def record(diagnostic: ServerDiagnostic): Unit =
      diagnostic match {
        case RequestCompleted(protocol, connector, method, path, status, durationMs) =>
          trace.span("http.request", SpanKind.Server) { span =>
            span.setAttribute("http.request.method", method)
            span.setAttribute("url.path", path)
            span.setAttribute("network.protocol.name", protocol)
            span.setAttribute("network.protocol.connector", connector)
            span.setAttribute("http.response.status_code", status.toLong)
            span.setAttribute("http.server.active_connections", activeCount.get())
            span.setAttribute("http.server.duration_ms", durationMs)
          }
          requestsTotal.add(
            1L,
            "method"      -> method,
            "path"        -> path,
            "status"      -> status,
            "protocol"    -> protocol,
            "connector"   -> connector,
          )
          logger.info(
            "HTTP request",
            "method"      -> AttributeValue.StringValue(method),
            "path"        -> AttributeValue.StringValue(path),
            "status"      -> AttributeValue.LongValue(status.toLong),
            "duration_ms" -> AttributeValue.LongValue(durationMs),
            "protocol"    -> AttributeValue.StringValue(protocol),
            "connector"   -> AttributeValue.StringValue(connector),
          )
        case ParseRejected(protocol, connector, errorType, status)                   =>
          trace.span("http.request", SpanKind.Server) { span =>
            span.setAttribute("network.protocol.name", protocol)
            span.setAttribute("network.protocol.connector", connector)
            span.setAttribute("error.type", errorType)
            status.foreach(code => span.setAttribute("http.response.status_code", code.toLong))
          }
          rejectedTotal.add(
            1L,
            "protocol"   -> protocol,
            "connector"  -> connector,
            "error_type" -> errorType,
          )
          logger.warn(
            "HTTP request rejected",
            "protocol"   -> AttributeValue.StringValue(protocol),
            "connector"  -> AttributeValue.StringValue(connector),
            "error_type" -> AttributeValue.StringValue(errorType),
            "status"     -> AttributeValue.LongValue(status.getOrElse(-1).toLong),
          )
        case RequestTimeout(protocol, connector, timeoutKind, errorType)             =>
          trace.span("http.request", SpanKind.Server) { span =>
            span.setAttribute("network.protocol.name", protocol)
            span.setAttribute("network.protocol.connector", connector)
            span.setAttribute("http.request.timeout_kind", timeoutKind.value)
            span.setAttribute("error.type", errorType)
          }
          timeoutTotal.add(
            1L,
            "protocol"     -> protocol,
            "connector"    -> connector,
            "timeout_kind" -> timeoutKind.value,
          )
          logger.warn(
            "HTTP request timeout",
            "protocol"     -> AttributeValue.StringValue(protocol),
            "connector"    -> AttributeValue.StringValue(connector),
            "timeout_kind" -> AttributeValue.StringValue(timeoutKind.value),
            "error_type"   -> AttributeValue.StringValue(errorType),
          )
        case StreamReset(protocol, connector, reason, streamId)                      =>
          resetTotal.add(
            1L,
            "protocol"  -> protocol,
            "connector" -> connector,
            "reason"    -> reason.value,
          )
          logger.warn(
            "HTTP stream reset",
            "protocol"  -> AttributeValue.StringValue(protocol),
            "connector" -> AttributeValue.StringValue(connector),
            "reason"    -> AttributeValue.StringValue(reason.value),
            "stream_id" -> AttributeValue.LongValue(streamId.getOrElse(-1).toLong),
          )
        case ConnectionOpened(protocol, connector)                                   =>
          activeCount.incrementAndGet()
          activeConnections.add(1L, "protocol" -> protocol, "connector" -> connector)
          logger.debug(
            "HTTP connection opened",
            "protocol"                         -> AttributeValue.StringValue(protocol),
            "connector"                        -> AttributeValue.StringValue(connector),
          )
        case ConnectionClosed(protocol, connector, drained)                          =>
          activeCount.decrementAndGet()
          activeConnections.add(-1L, "protocol" -> protocol, "connector" -> connector)
          closedTotal.add(
            1L,
            "protocol"                          -> protocol,
            "connector"                         -> connector,
            "drained"                           -> drained.toString,
          )
          logger.debug(
            "HTTP connection closed",
            "protocol"                          -> AttributeValue.StringValue(protocol),
            "connector"                         -> AttributeValue.StringValue(connector),
            "drained"                           -> AttributeValue.StringValue(drained.toString),
          )
        case BindFailure(connector, errorType)                                       =>
          bindFailuresTotal.add(
            1L,
            "connector"  -> connector,
            "error_type" -> errorType,
          )
          logger.error(
            "HTTP bind failure",
            "connector"  -> AttributeValue.StringValue(connector),
            "error_type" -> AttributeValue.StringValue(errorType),
          )
        case NegotiationFailure(connector, failure)                                  =>
          val failureType    = failure.getClass.getSimpleName
          negotiationFailuresTotal.add(
            1L,
            "connector"    -> connector,
            "failure_type" -> failureType,
          )
          logger.error(
            "HTTP negotiation failure",
            "connector"    -> AttributeValue.StringValue(connector),
            "failure_type" -> AttributeValue.StringValue(failureType),
            "detail"       -> AttributeValue.StringValue(ServerTelemetry.sanitizeToken(failure.message)),
          )
      }
  }
}

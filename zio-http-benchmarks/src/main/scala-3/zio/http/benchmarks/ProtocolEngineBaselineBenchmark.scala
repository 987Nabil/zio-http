/*
 * Copyright 2026 the ZIO HTTP contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio.http.benchmarks

import java.util.concurrent.TimeUnit

import scala.compiletime.uninitialized

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import zio.blocks.chunk.Chunk
import zio.blocks.context.Context
import zio.blocks.endpoint.RoutePattern
import zio.blocks.scope.{Scope => BlocksScope}

import zio.http.{DefectHandler, EngineDispatcher, Handler, Request, Response, Route, Routes, URL}
import zio.http.h1.{H1BodyFraming, H1Decoder, H1Encoder, H1Header, H1Headers, H1Response}
import zio.http.h2.{FrameCodec, H2Frame, Setting}

/**
 * Todo 18 — controlled protocol-engine performance and allocation baselines.
 *
 * Covers the four required dimensions with in-process (no-socket) workloads so
 * the run is reproducible on a shared host:
 *   - H1 direct codec: [[h1ParseGet]] / [[h1ParsePostFixed]] (parse) and
 *     [[h1EncodeResponse]] (encode). The parse benchmarks feed a per-thread
 *     [[H1Decoder]] reused across invocations, matching keep-alive reuse; the
 *     H1 direct-codec budget in [[ProtocolBaselineThresholds]] is derived from
 *     this controlled run.
 *   - Existing H2 baseline: [[h2EncodePing]] / [[h2DecodePing]] /
 *     [[h2EncodeSettings]] exercise the merged H2 frame codec (no HPACK path,
 *     which belongs to the connection layer, not the frame baseline).
 *   - Shared dispatch overhead: [[dispatchShared]] (full
 *     [[EngineDispatcher.dispatch]]: tree lookup, pattern decode, scope
 *     open/close) versus [[dispatchDirect]] (the same handler invoked directly
 *     — the no-lookup floor). The overhead gate compares these two methods from
 *     the SAME run.
 *   - Warm serving and concurrent traffic: every method is measured
 *     steady-state after warmup (warm serving); [[dispatchSharedConcurrent]]
 *     adds the 4-thread concurrent-traffic shape over the same dispatcher.
 *   - p99 tail latency: [[h2EncodePingP99]] / [[h2DecodePingP99]] /
 *     [[dispatchSharedP99]] / [[dispatchDirectP99]] run in SampleTime mode so
 *     the H2 p99 regression gate and the dispatch overhead gate are evaluated
 *     on tail latency as well as on AverageTime.
 *
 * Fixed corpus (pinned here, repeated in the evidence log): a GET with three
 * small headers, a POST with `Content-Length: 11` and an 11-byte body, a 200
 * response with `Content-Length: 11`, an H2 PING and an H2 SETTINGS frame, and
 * a single root `GET /` route whose handler derives a small text body from the
 * request path (minimal realistic per-request allocation, shared by both
 * dispatch benchmarks so their delta isolates lookup cost).
 *
 * Controlled-run invocation (see also the evidence log for the exact command
 * and host/JDK/GC metadata):
 * {{{
 * ./mill 'benchmarks.jvm[3.9.0].runJmh' zio.http.benchmarks.ProtocolEngineBaselineBenchmark -prof gc -rf json -rff protocol-baseline.json
 * }}}
 *
 * Host-specificity warning: scores and `gc.alloc.rate.norm` values from any run
 * are valid ONLY for the host/JDK/GC recorded alongside them. Never
 * universalize them; compare runs only via [[BaselineRegressionCheck]], which
 * enforces the 99%-CI separation rule.
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
class ProtocolEngineBaselineBenchmark {

  // Fixed wire corpus. Non-final @State fields (assigned in @Setup) so JMH
  // cannot constant-fold the inputs.
  private var getWire: Chunk[Byte]              = uninitialized
  private var postWire: Chunk[Byte]             = uninitialized
  private var pingBytes: Chunk[Byte]            = uninitialized
  private var decoder: H1Decoder                = uninitialized
  private var response: H1Response              = uninitialized
  private var ping: H2Frame.Ping                = uninitialized
  private var settings: H2Frame.Settings        = uninitialized
  private var dispatcher: EngineDispatcher[Any] = uninitialized
  private var directHandler: Handler[Any, Any]  = uninitialized
  private var dispatchRequest: Request          = uninitialized

  @Setup(Level.Trial)
  def setup(): Unit = {
    getWire = Chunk.fromArray(
      "GET /hello HTTP/1.1\r\nHost: example.com\r\nUser-Agent: zio-http-bench\r\nAccept: */*\r\n\r\n".getBytes(
        "ISO-8859-1",
      ),
    )
    postWire = Chunk.fromArray(
      "POST /echo HTTP/1.1\r\nHost: example.com\r\nContent-Length: 11\r\n\r\nhello world".getBytes("ISO-8859-1"),
    )
    decoder = new H1Decoder()
    val body  = Chunk.fromArray("hello world".getBytes("ISO-8859-1"))
    response = H1Response(
      status = 200,
      reason = "OK",
      headers = H1Headers(List(H1Header("Content-Length", "11"))),
      body = body,
      framing = H1BodyFraming.Fixed(11L),
      trailers = H1Headers.Empty,
    )
    ping = H2Frame.Ping(ack = false, Chunk.fromArray(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7)))
    pingBytes = FrameCodec.encode(ping)
    settings = H2Frame.Settings(ack = false, List(Setting(1, 100L), Setting(4, 65535L)))
    // Minimal realistic app work shared by BOTH dispatch benchmarks so their
    // delta isolates shared-lookup cost: derive the body from the request
    // (forces a per-request allocation, as any real handler does) instead of
    // returning a prebuilt constant (which the JIT folds to ~0.5ns and would
    // make the overhead ratio meaningless).
    val route = Route(RoutePattern.GET, Handler.fromRequest(request => Response.text("ok:" + request.path)))
    directHandler = route.handler
    dispatcher = new EngineDispatcher(Routes(route), Context.empty, DefectHandler.default)
    dispatchRequest = Request.get(URL.root)
  }

  @TearDown(Level.Iteration)
  def checkDecoder(): Unit =
    if (decoder.poisoned)
      throw new IllegalStateException("H1Decoder poisoned during benchmark — scores would be invalid")

  @Benchmark
  def h1ParseGet(bh: Blackhole): Unit =
    bh.consume(decoder.feed(getWire))

  @Benchmark
  def h1ParsePostFixed(bh: Blackhole): Unit =
    bh.consume(decoder.feed(postWire))

  @Benchmark
  def h1EncodeResponse(bh: Blackhole): Unit =
    bh.consume(H1Encoder.encodeResponse(response))

  @Benchmark
  def h2EncodePing(bh: Blackhole): Unit =
    bh.consume(FrameCodec.encode(ping))

  @Benchmark
  def h2DecodePing(bh: Blackhole): Unit =
    bh.consume(FrameCodec.decode(pingBytes))

  @Benchmark
  def h2EncodeSettings(bh: Blackhole): Unit =
    bh.consume(FrameCodec.encode(settings))

  @Benchmark
  def dispatchShared(bh: Blackhole): Unit =
    bh.consume(dispatcher.dispatch(dispatchRequest))

  @Benchmark
  def dispatchDirect(bh: Blackhole): Unit =
    bh.consume(directHandler.handle(dispatchRequest, Context.empty, (), BlocksScope.global))

  @Benchmark
  @Threads(4)
  def dispatchSharedConcurrent(bh: Blackhole): Unit =
    bh.consume(dispatcher.dispatch(dispatchRequest))

  /**
   * p99 tail-latency shapes (SampleTime): the H2 frame path in both directions
   * and the dispatch pair, so the H2 p99 regression gate and the dispatch
   * overhead gate can be evaluated on tail latency as well as on AverageTime.
   * Fewer measurement iterations than the AverageTime methods: p99 needs
   * samples, not long means.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  def h2EncodePingP99(bh: Blackhole): Unit =
    bh.consume(FrameCodec.encode(ping))

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  def h2DecodePingP99(bh: Blackhole): Unit =
    bh.consume(FrameCodec.decode(pingBytes))

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  def dispatchSharedP99(bh: Blackhole): Unit =
    bh.consume(dispatcher.dispatch(dispatchRequest))

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  @Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
  def dispatchDirectP99(bh: Blackhole): Unit =
    bh.consume(directHandler.handle(dispatchRequest, Context.empty, (), BlocksScope.global))
}

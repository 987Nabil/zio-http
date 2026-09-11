package zio.http.h1

import scala.annotation.experimental

import zio.blocks.chunk.Chunk
import zio.test._

/**
 * Todo 19 publish contract for the `zio-http-h1-codec` leaf artifact.
 *
 * Consumer's-eye view of the published leaf: request decode, response encode,
 * and encode/decode round-trips — the exact wire behavior downstream consumers
 * depend on. The dependency direction (leaf depends only on `zio-blocks-chunk`;
 * core/server/Loom never leak into the codec) is pinned at the Mill level:
 * `h1Codec.jvm().mvnDeps` lists only `blocksDep("chunk")` and no `moduleDeps`
 * entry points at the codec from core/server (only `serverLoom` depends on it,
 * transitively serving engines).
 */
@experimental
object H1CodecPublishContractSpec extends ZIOSpecDefault {

  private def wire(s: String): Chunk[Byte] =
    Chunk.fromArray(s.getBytes("US-ASCII"))

  override def spec =
    suite("H1CodecPublishContractSpec")(
      test("leaf decodes a GET request for a direct consumer") {
        val decoder = new H1Decoder()
        val result  = decoder.feed(wire("GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n"))
        assertTrue(
          result == Right(
            List(
              H1Request(
                method = "GET",
                target = "/hello",
                headers = H1Headers(List(H1Header("Host", "example.com"))),
                body = Chunk.empty,
                framing = H1BodyFraming.Empty,
                trailers = H1Headers.Empty,
              ),
            ),
          ),
        )
      },
      test("leaf encodes a response for a direct consumer") {
        val response = H1Response(
          status = 200,
          reason = "OK",
          headers = H1Headers(List(H1Header("Content-Length", "5"))),
          body = wire("hello"),
          framing = H1BodyFraming.Fixed(5L),
          trailers = H1Headers.Empty,
        )
        val decoder  = new H1Decoder()
        H1Encoder.encodeResponse(response).flatMap(decoder.feedResponse(_, "GET")) match {
          case Right(List(decoded)) =>
            assertTrue(decoded.status == 200, decoded.body == wire("hello"))
          case other                =>
            assertTrue(false)
        }
      },
      test("request encode inverts decode for a direct consumer") {
        val request = H1Request(
          method = "POST",
          target = "/submit",
          headers = H1Headers(List(H1Header("Host", "x"), H1Header("Content-Length", "5"))),
          body = wire("hello"),
          framing = H1BodyFraming.Fixed(5L),
          trailers = H1Headers.Empty,
        )
        val decoder = new H1Decoder()
        assertTrue(H1Encoder.encodeRequest(request).flatMap(decoder.feed) == Right(List(request)))
      },
    )
}

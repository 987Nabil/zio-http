package zio.http.conformance

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.experimental

import zio.blocks.chunk.Chunk
import zio.blocks.endpoint.RoutePattern
import zio.blocks.streams.Stream

import zio.http.ResultType._
import zio.http.sse.{ServerSentEvent, Sse}
import zio.http.{
  Body,
  DefectHandler,
  Handler,
  Halt,
  Header,
  Headers,
  Method,
  Request,
  Response,
  Route,
  Routes,
  Status,
  handler,
}

/**
 * Todo 10: the single shared route corpus every engine must serve identically.
 *
 * One application definition covers methods, paths, query strings, request and
 * response headers, request/response bodies, streamed bodies both directions,
 * SSE, cookies, token sessions, defects, halts, and the cancellation target.
 * Raw-wire and security cases are deliberately absent: Todo 15 owns them.
 *
 * Uniformity notes (no per-protocol branches anywhere in the harness):
 *   - HEAD responses carry no body on any engine (RFC 9110 section 9.3.2; the
 *     H2 transport already strips it, the H1 engine in Todo 7 must too).
 *   - `Secure` is omitted from Set-Cookie: the corpus runs over cleartext and
 *     must stay meaningful for H1 cleartext as well as H2C.
 */
@experimental
object ConformanceCorpus {

  val StreamDownloadTotal: Int = 96 * 1024
  val StreamUploadTotal: Int   = 32 * 1024
  val CancelTotal: Int         = 256 * 1024
  val EchoBodyTotal: Int       = 1024

  /** Deterministic binary payload: byte i is (i % 251). */
  def deterministicBytes(total: Int): Chunk[Byte] = {
    val arr = new Array[Byte](total)
    var i   = 0
    while (i < total) {
      arr(i) = ((i % 251) & 0xff).toByte
      i += 1
    }
    Chunk.fromArray(arr)
  }

  /** Unsigned-byte sum over exact bytes; both directions assert with this. */
  def checksum(bytes: Chunk[Byte]): Long = {
    val arr = bytes.toArray
    var sum = 0L
    var i   = 0
    while (i < arr.length) {
      sum += (arr(i) & 0xff).toLong
      i += 1
    }
    sum
  }

  /**
   * Lazily-generated deterministic bytes: O(1) source memory, unknown length.
   */
  def unfoldingBytes(total: Int): Stream[Nothing, Byte] =
    Stream.unfold(0) { i =>
      if (i >= total) None
      else Some((((i % 251) & 0xff).toByte, i + 1))
    }

  /**
   * Session-token mint: 32 SecureRandom bytes, Base64-URL, no padding (43
   * chars).
   */
  def mintToken(): String = {
    val bytes = new Array[Byte](32)
    ConformanceCorpus.secureRandom.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding.encodeToString(bytes)
  }

  private val secureRandom = new SecureRandom()

  /** Live session store: token -> user. Tests are self-contained per login. */
  val sessions: ConcurrentHashMap[String, String] =
    new ConcurrentHashMap[String, String]()

  def sessionFrom(headers: Headers): Option[String] =
    headers.rawGet("Cookie").flatMap { raw =>
      raw.split(";").map(_.trim).find(_.startsWith("session=")).map(_.substring("session=".length))
    }

  val sseEvents: Stream[Nothing, ServerSentEvent] =
    Stream.unfold(0) { i =>
      if (i >= 3) None
      else Some((ServerSentEvent("e" + i), i + 1))
    }

  def expectedSseText: String =
    (0 until 3)
      .map(i => new String(zio.http.sse.SseCodec.encode(ServerSentEvent("e" + i)).toArray, StandardCharsets.UTF_8))
      .mkString

  /** Custom defect mapping used by the typed-failure leg: IAE -> 400. */
  val mappingDefects: DefectHandler = new DefectHandler {
    def handleDefect(request: Request, throwable: Throwable): Response | Halt =
      throwable match {
        case _: IllegalArgumentException =>
          responseAsResult(Response(status = Status.BadRequest, body = Body.fromString("mapped:bad-input")))
        case _                           =>
          responseAsResult(Response.internalServerError)
      }
  }

  val routes: Routes[Any] = Routes(
    Route(
      RoutePattern(Method.GET, "/methods/get"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("get-ok")))
      },
    ),
    Route(
      RoutePattern(Method.POST, "/methods/users"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Created, body = Body.fromString("created")))
      },
    ),
    Route(
      RoutePattern(Method.PUT, "/methods/put"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("put-ok")))
      },
    ),
    Route(
      RoutePattern(Method.DELETE, "/methods/item"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("delete-ok")))
      },
    ),
    Route(
      RoutePattern(Method.PATCH, "/methods/patch"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("patch-ok")))
      },
    ),
    Route(
      RoutePattern(Method.OPTIONS, "/methods/options"),
      handler { (_: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("options-ok"))
            .addHeader(Header.Custom("Allow", "GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD")),
        )
      },
    ),
    Route(
      RoutePattern(Method.HEAD, "/methods/head"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("head-body-stripped")))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/path/users/42"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("user-42")))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/path/nested/deep"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("deep-ok")))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/query"),
      handler { (req: Request) =>
        val active = req.url.queryParams.getFirst("active").getOrElse("<missing>")
        val mode   = req.url.queryParams.getFirst("mode").getOrElse("<missing>")
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("active=" + active + "|mode=" + mode)))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/header/echo"),
      handler { (req: Request) =>
        val echoed = req.headers.rawGet("X-Echo").getOrElse("<missing>")
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("echo:" + echoed))
            .addHeader(Header.Custom("X-Conformance", "harness")),
        )
      },
    ),
    Route(
      RoutePattern(Method.POST, "/body/echo"),
      handler { (req: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromChunk(req.body.toChunk)))
      },
    ),
    Route(
      RoutePattern(Method.POST, "/body/empty"),
      handler { (req: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromString("empty:" + req.body.toChunk.length)))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/stream/big"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromStream(unfoldingBytes(StreamDownloadTotal))))
      },
    ),
    Route(
      RoutePattern(Method.POST, "/stream/upload"),
      handler { (req: Request) =>
        val bytes = req.body.toChunk
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("upload:" + bytes.length + ":" + checksum(bytes))),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/sse"),
      handler { (_: Request) =>
        responseAsResult(Sse.response(sseEvents))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/cookie/set"),
      handler { (_: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("cookie-set"))
            .addHeader(Header.Custom("Set-Cookie", "flavor=oatmeal; Path=/; HttpOnly; SameSite=Strict")),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/cookie/echo"),
      handler { (req: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString(req.headers.rawGet("Cookie").getOrElse("<none>"))),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/cookie/clear"),
      handler { (_: Request) =>
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("cookie-cleared"))
            .addHeader(Header.Custom("Set-Cookie", "flavor=; Path=/; Max-Age=0")),
        )
      },
    ),
    Route(
      RoutePattern(Method.POST, "/session/login"),
      handler { (_: Request) =>
        val token = mintToken()
        sessions.put(token, "user")
        responseAsResult(
          Response(status = Status.Ok, body = Body.fromString("logged-in"))
            .addHeader(Header.Custom("Set-Cookie", "session=" + token + "; Path=/; HttpOnly; SameSite=Strict")),
        )
      },
    ),
    Route(
      RoutePattern(Method.GET, "/session/me"),
      handler { (req: Request) =>
        sessionFrom(req.headers) match {
          case Some(token) if sessions.containsKey(token) =>
            responseAsResult(Response(status = Status.Ok, body = Body.fromString("me:" + token)))
          case _                                          =>
            responseAsResult(Response(status = Status.Forbidden, body = Body.fromString("anonymous")))
        }
      },
    ),
    Route(
      RoutePattern(Method.GET, "/defect/boom"),
      Handler.fromRequest { (_: Request) =>
        throw new RuntimeException("boom")
      },
    ),
    Route(
      RoutePattern(Method.GET, "/defect/halt"),
      handler { (_: Request) =>
        haltAsResult(Halt(Response(status = Status.Forbidden, body = Body.fromString("forbidden-halt"))))
      },
    ),
    Route(
      RoutePattern(Method.GET, "/defect/mapped"),
      Handler.fromRequest { (_: Request) =>
        throw new IllegalArgumentException("bad-input")
      },
    ),
    Route(
      RoutePattern(Method.GET, "/cancel/big"),
      handler { (_: Request) =>
        responseAsResult(Response(status = Status.Ok, body = Body.fromChunk(deterministicBytes(CancelTotal))))
      },
    ),
  )
}

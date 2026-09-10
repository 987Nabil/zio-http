package zio.http.conformance

import java.nio.charset.StandardCharsets

import scala.annotation.experimental

import zio.Scope
import zio.ZIO
import zio.blocks.chunk.Chunk
import zio.test._

/**
 * Todo 10: the shared conformance runner.
 *
 * [[suiteFor]] executes the whole [[ConformanceCorpus]] against one backend.
 * Every assertion compares `(engineTag, actual)` against `(engineTag,
 * expected)` tuples, so a failure diagnostic always names the engine under test
 * even when H1 and H2C legs report side by side. There are no per-protocol
 * branches: any future intentional wire difference must be declared here with
 * its RFC reason, never silently tolerated.
 */
@experimental
object ConformanceHarness {

  def suiteFor(backend: ConformanceBackend): Spec[TestEnvironment with Scope, Any] =
    suite("conformance[" + backend.tag.label + "]")(
      test("[" + backend.tag.label + "] methods: GET/POST/PUT/DELETE/PATCH serve their statuses") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label = backend.tag.label
            val get   = client.request("GET", "/methods/get", Nil, Chunk.empty)
            val post  = client.request("POST", "/methods/users", Nil, Chunk.empty)
            val put = client.request("PUT", "/methods/put", Nil, Chunk.fromArray("p".getBytes(StandardCharsets.UTF_8)))
            val delete = client.request("DELETE", "/methods/item", Nil, Chunk.empty)
            val patch  = client.request("PATCH", "/methods/patch", Nil, Chunk.empty)
            assertTrue(
              (label, get.status) == ((label, 200)),
              (label, get.bodyText) == ((label, "get-ok")),
              (label, post.status) == ((label, 201)),
              (label, post.bodyText) == ((label, "created")),
              (label, put.status) == ((label, 200)),
              (label, put.bodyText) == ((label, "put-ok")),
              (label, delete.status) == ((label, 200)),
              (label, delete.bodyText) == ((label, "delete-ok")),
              (label, patch.status) == ((label, 200)),
              (label, patch.bodyText) == ((label, "patch-ok")),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] options advertises Allow; HEAD is status-only with no body") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label   = backend.tag.label
            val options = client.request("OPTIONS", "/methods/options", Nil, Chunk.empty)
            val head    = client.request("HEAD", "/methods/head", Nil, Chunk.empty)
            assertTrue(
              (label, options.status) == ((label, 200)),
              (label, options.bodyText) == ((label, "options-ok")),
              (label, options.headerFirst("allow")) == ((label, Some("GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD"))),
              (label, head.status) == ((label, 200)),
              (label, head.body.isEmpty) == ((label, true)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] paths: literals match, missing paths 404") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label   = backend.tag.label
            val user    = client.request("GET", "/path/users/42", Nil, Chunk.empty)
            val deep    = client.request("GET", "/path/nested/deep", Nil, Chunk.empty)
            val missing = client.request("GET", "/no-such-route", Nil, Chunk.empty)
            assertTrue(
              (label, user.status) == ((label, 200)),
              (label, user.bodyText) == ((label, "user-42")),
              (label, deep.status) == ((label, 200)),
              (label, deep.bodyText) == ((label, "deep-ok")),
              (label, missing.status) == ((label, 404)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] query: params decode to handler values") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label = backend.tag.label
            val resp  = client.request("GET", "/query?active=true&mode=fast", Nil, Chunk.empty)
            val bare  = client.request("GET", "/query", Nil, Chunk.empty)
            assertTrue(
              (label, resp.status) == ((label, 200)),
              (label, resp.bodyText) == ((label, "active=true|mode=fast")),
              (label, bare.status) == ((label, 200)),
              (label, bare.bodyText) == ((label, "active=<missing>|mode=<missing>")),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] headers: request echo plus response marker survive") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label = backend.tag.label
            val resp  = client.request("GET", "/header/echo", List("X-Echo" -> "hello-header"), Chunk.empty)
            assertTrue(
              (label, resp.status) == ((label, 200)),
              (label, resp.bodyText) == ((label, "echo:hello-header")),
              (label, resp.headerFirst("x-conformance")) == ((label, Some("harness"))),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] body: binary echo is byte-exact, empty posts report zero") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label   = backend.tag.label
            val payload = ConformanceCorpus.deterministicBytes(ConformanceCorpus.EchoBodyTotal)
            val echo    = client.request("POST", "/body/echo", Nil, payload)
            val empty   = client.request("POST", "/body/empty", Nil, Chunk.empty)
            assertTrue(
              (label, echo.status) == ((label, 200)),
              (label, echo.body) == ((label, payload)),
              (label, empty.status) == ((label, 200)),
              (label, empty.bodyText) == ((label, "empty:0")),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] streaming: unknown-length download is byte-exact") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label    = backend.tag.label
            val total    = ConformanceCorpus.StreamDownloadTotal
            val expected = ConformanceCorpus.deterministicBytes(total)
            val resp     = client.request("GET", "/stream/big", Nil, Chunk.empty)
            assertTrue(
              (label, resp.status) == ((label, 200)),
              (label, resp.body.length) == ((label, total)),
              (label, resp.body) == ((label, expected)),
              (label, ConformanceCorpus.checksum(resp.body)) == ((label, ConformanceCorpus.checksum(expected))),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] streaming: upload length and checksum round-trip") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label    = backend.tag.label
            val payload  = ConformanceCorpus.deterministicBytes(ConformanceCorpus.StreamUploadTotal)
            val expected = "upload:" + payload.length + ":" + ConformanceCorpus.checksum(payload)
            val resp     = client.request("POST", "/stream/upload", Nil, payload)
            assertTrue(
              (label, resp.status) == ((label, 200)),
              (label, resp.bodyText) == ((label, expected)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] SSE: events arrive content-typed and byte-exact") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label    = backend.tag.label
            val expected = ConformanceCorpus.expectedSseText
            val resp     = client.request("GET", "/sse", Nil, Chunk.empty)
            assertTrue(
              (label, resp.status) == ((label, 200)),
              (label, resp.headerFirst("content-type").exists(_.contains("text/event-stream"))) == ((label, true)),
              (label, resp.bodyText) == ((label, expected)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] cookie: set/echo/clear round-trip") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label     = backend.tag.label
            val set       = client.request("GET", "/cookie/set", Nil, Chunk.empty)
            val setCookie = set.headerFirst("set-cookie").getOrElse("")
            val echo      = client.request("GET", "/cookie/echo", List("Cookie" -> "flavor=oatmeal"), Chunk.empty)
            val none      = client.request("GET", "/cookie/echo", Nil, Chunk.empty)
            val clear     = client.request("GET", "/cookie/clear", Nil, Chunk.empty)
            assertTrue(
              (label, set.status) == ((label, 200)),
              (label, set.bodyText) == ((label, "cookie-set")),
              (label, setCookie.contains("flavor=oatmeal")) == ((label, true)),
              (label, setCookie.contains("HttpOnly")) == ((label, true)),
              (label, setCookie.contains("SameSite=Strict")) == ((label, true)),
              (label, echo.bodyText) == ((label, "flavor=oatmeal")),
              (label, none.bodyText) == ((label, "<none>")),
              (label, clear.status) == ((label, 200)),
              (label, clear.headerFirst("set-cookie").exists(_.contains("Max-Age=0"))) == ((label, true)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] session: login mints a token, bearer sees self, stranger is refused") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label     = backend.tag.label
            val login     = client.request("POST", "/session/login", Nil, Chunk.empty)
            val setCookie = login.headerFirst("set-cookie").getOrElse("")
            val token     = setCookie
              .split(";")
              .map(_.trim)
              .find(_.startsWith("session="))
              .map(_.substring("session=".length))
              .getOrElse("")
            val me        = client.request("GET", "/session/me", List("Cookie" -> ("session=" + token)), Chunk.empty)
            val stranger  =
              client.request("GET", "/session/me", List("Cookie" -> "session=not-a-real-token"), Chunk.empty)
            val anonymous = client.request("GET", "/session/me", Nil, Chunk.empty)
            assertTrue(
              (label, login.status) == ((label, 200)),
              (label, login.bodyText) == ((label, "logged-in")),
              (label, token.length) == ((label, 43)),
              (label, me.status) == ((label, 200)),
              (label, me.bodyText) == ((label, "me:" + token)),
              (label, stranger.status) == ((label, 403)),
              (label, anonymous.status) == ((label, 403)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] defect: throwing handler is a 500, halt is its own response") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label = backend.tag.label
            val boom  = client.request("GET", "/defect/boom", Nil, Chunk.empty)
            val halt  = client.request("GET", "/defect/halt", Nil, Chunk.empty)
            assertTrue(
              (label, boom.status) == ((label, 500)),
              (label, halt.status) == ((label, 403)),
              (label, halt.bodyText) == ((label, "forbidden-halt")),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] defect: custom handler maps typed failures before the engine") {
        backend.withServerAndDefects(ConformanceCorpus.routes, ConformanceCorpus.mappingDefects) { client =>
          ZIO.attemptBlocking {
            val label  = backend.tag.label
            val mapped = client.request("GET", "/defect/mapped", Nil, Chunk.empty)
            val boom   = client.request("GET", "/defect/boom", Nil, Chunk.empty)
            assertTrue(
              (label, mapped.status) == ((label, 400)),
              (label, mapped.bodyText) == ((label, "mapped:bad-input")),
              (label, boom.status) == ((label, 500)),
            )
          }
        }
      },
      test("[" + backend.tag.label + "] cancellation: aborted stream leaves no stale state behind") {
        backend.withServer(ConformanceCorpus.routes) { client =>
          ZIO.attemptBlocking {
            val label    = backend.tag.label
            val total    = ConformanceCorpus.CancelTotal
            val expected = ConformanceCorpus.deterministicBytes(total)
            val aborted  = client.getThenAbort("/cancel/big")
            val again    = client.request("GET", "/cancel/big", Nil, Chunk.empty)
            val clean    = client.request("GET", "/methods/get", Nil, Chunk.empty)
            assertTrue(
              (label, aborted.status) == ((label, 200)),
              (label, aborted.firstBytes.nonEmpty) == ((label, true)),
              (label, aborted.firstBytes.length < total) == ((label, true)),
              (label, expected.toArray.take(aborted.firstBytes.length).toSeq) == ((
                label,
                aborted.firstBytes.toArray.toSeq,
              )),
              (label, again.status) == ((label, 200)),
              (label, again.body) == ((label, expected)),
              (label, clean.status) == ((label, 200)),
              (label, clean.bodyText) == ((label, "get-ok")),
            )
          }
        }
      },
    )
}

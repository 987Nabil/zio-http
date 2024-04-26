package zio.http

import zio._
import zio.test._

object ForwardHeaderSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ForwardHeaderSpec")(
      test("should forward headers") {
        val routes = Routes(
          Method.GET / "get"   -> handler((_: Request) =>
            for {
              client   <- ZIO.service[Client]
              response <- (client @@ ZClientAspect.forwardHeaders)
                .request(Request.post(url"http://localhost:8080/post", Body.empty))
            } yield response,
          ),
          Method.POST / "post" -> handler((req: Request) => Response.ok.addHeader(req.header(Header.Accept).get)),
        ).sandbox @@ Middleware.forwardHeaders(Header.Accept)

        for {
          _        <- Server.install(routes)
          response <- Client.request(
            Request.get(url"http://localhost:8080/get").addHeader(Header.Accept(MediaType.application.json)),
          )
        } yield assertTrue(response.headers(Header.Accept).contains(Header.Accept(MediaType.application.json)))
      },
    ).provideSome[Scope](Client.default, Server.default) @@ TestAspect.withLiveClock

}

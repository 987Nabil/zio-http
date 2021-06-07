package zhttp.service

import zhttp.http._
import zhttp.service.server.ServerSslHandler.SslOptions.SelfSigned
import zhttp.service.server._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.assertM

object HttpsServerSpec extends HttpRunnableSpec(8081) {
  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto

  val app = serve(
    HttpApp.collectM {
      case Method.GET -> Root / "success" => ZIO.succeed(Response.ok)
      case Method.GET -> Root / "failure" => ZIO.fail(new RuntimeException("FAILURE"))
    },
    SelfSigned,
  )

  override def spec = suiteM("Server")(
    app
      .as(
        List(
          testM("200 response") {
            val actual = Client.request("https://localhost:8081/success").map(_.status)
            assertM(actual)(equalTo(Status.OK))
          },
          // TODO: to be implemented after getting ssl support for client
        ),
      )
      .useNow,
  ).provideCustomLayer(env)
}

package zio.http

import zio._

trait HandlerVersionSpecific {
  private[http] class ApplyContextAspect[-Env, +Err, -In, +Out, Env0](private val self: Handler[Env, Err, In, Out]) {
    def apply[Env1, Ctx: Tag, In1 <: In](aspect: HandlerAspect[Env1, Ctx])(implicit
      ev0: Request <:< In,
      ev: Env0 with Ctx <:< Env,
      out: Out <:< Response,
      err: Err <:< Response,
      trace: Trace,
    ): Handler[Env0 with Env1, Response, Request, Response] =
      aspect.applyHandlerContext {
        handler { (ctx: Ctx, req: Request) =>
          val handler: ZIO[Env, Response, Response] = self.asInstanceOf[Handler[Env, Response, Request, Response]](req)
          handler.provideSomeEnvironment[Env0](_.add[Ctx](ctx).asInstanceOf[ZEnvironment[Env]])
        }
      }
  }

}

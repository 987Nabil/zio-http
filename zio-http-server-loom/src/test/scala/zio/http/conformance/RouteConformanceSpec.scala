package zio.http.conformance

import scala.annotation.experimental

import zio._
import zio.test.TestAspect.sequential
import zio.test._

/**
 * Todo 10: protocol-neutral route conformance, live H2C leg.
 *
 * The identical corpus ([[ConformanceHarness.suiteFor]]) executes per engine;
 * this leg runs it against H2C today. The H1 leg wires into the same builder
 * via [[H1Backend]] once Todo 7 lands the Loom H1 engine. Raw-wire and security
 * cases stay outside this harness by design (Todo 15 owns them).
 */
@experimental
object RouteConformanceSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    ConformanceHarness.suiteFor(H2CBackend) @@ sequential
}

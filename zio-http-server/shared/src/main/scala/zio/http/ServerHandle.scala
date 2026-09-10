package zio.http

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Handle to a running server.
 *
 * Unsealed (Todo 8) so the protocol-independent
 * [[zio.http.AggregateServerHandle]] (JVM Loom server) extends it: one `serve`
 * call starts every engine, one `shutdown` drains them all, and `awaitShutdown`
 * blocks until the terminal state instead of returning at once.
 */
abstract class ServerHandle extends AutoCloseable {
  def bindings: List[BoundConnector]
  def isRunning: Boolean
  def shutdown(): Unit
  def awaitShutdown(): Unit

  /**
   * Block up to `timeout` for the terminal state. Returns true when the handle
   * terminated in time.
   */
  def awaitShutdown(timeout: Duration): Boolean

  def shutdownAndWait(): Unit = {
    shutdown()
    awaitShutdown()
  }

  def registerShutdownHook(): this.type = {
    Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownAndWait()))
    this
  }

  override def close(): Unit = shutdownAndWait()
}

case class BoundConnector(address: BoundAddress, protocol: Protocol)

sealed trait BoundAddress
object BoundAddress {
  case class Tcp(host: String, port: Int)   extends BoundAddress
  case class Unix(path: java.nio.file.Path) extends BoundAddress
}

private[http] final case class BoundConnectorHandle(
  binding: BoundConnector,
  close0: () => Unit,
  isRunning0: () => Boolean,
  /**
   * Stop accepting new connections while established ones stay alive (Todo 8:
   * the aggregate drain phase). Bindings whose listener predates the
   * stop-accept hook use the full close (see the three-argument `apply`).
   */
  stopAccepting0: () => Unit,
)

private[http] object BoundConnectorHandle {

  /**
   * Bindings whose listener predates stop-accept: stopping falls back to the
   * full close.
   */
  def apply(
    binding: BoundConnector,
    close0: () => Unit,
    isRunning0: () => Boolean,
  ): BoundConnectorHandle =
    new BoundConnectorHandle(binding, close0, isRunning0, () => close0())
}

private[http] object ServerHandle {
  def live(bound: List[BoundConnectorHandle]): ServerHandle =
    new LiveServerHandle(bound)

  private final class LiveServerHandle(bound: List[BoundConnectorHandle]) extends ServerHandle {
    private val closed = new AtomicBoolean(false)

    override def bindings: List[BoundConnector] = bound.map(_.binding)

    override def isRunning: Boolean = !closed.get() && bound.forall(_.isRunning0())

    override def shutdown(): Unit =
      if (closed.compareAndSet(false, true)) bound.foreach(_.close0())

    override def awaitShutdown(): Unit = ()

    /**
     * The immediate handle has no drain phase: `awaitShutdown` already
     * returned, so the terminal state is trivially reached.
     */
    override def awaitShutdown(timeout: Duration): Boolean = true
  }
}

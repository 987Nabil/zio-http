package zio.http

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
 * Todo 8: aggregate multi-engine startup and graceful shutdown.
 *
 * This file wires the Todo 1 thick [[ProtocolEngine]] contract to the Todo 5
 * [[AggregateServerHandle]] lifecycle contract over the Todo 4
 * protocol-independent listener:
 *
 *   - [[QuiescentEngine]]: opt-in blocking drain for engines that track their
 *     in-flight connections (H1/H2). Engines without quiescence accounting keep
 *     the Todo 1 fire-and-forget `drain`/`close` hooks untouched.
 *   - [[InFlightTracker]]: lock-plus-`Condition` connection accounting with
 *     signaled quiescence — no polling, no sleeps. Entered on accept, exited
 *     when the connection handler returns.
 *   - [[BoundProtocolEngine]]: one bound engine as a [[LifecycleEngine]].
 *     `requestStop` stops the listener accept loop first (established
 *     connections stay alive) and then drains the engine; `awaitDrain` waits
 *     for quiescence under the aggregate's single deadline; `forceClose`
 *     force-closes the engine and releases the listener binding.
 */
trait QuiescentEngine {

  /**
   * Block up to `timeout` for in-flight connections to settle. Returns true
   * when the engine reached quiescence in time. Honors interrupts by returning
   * false with the interrupt flag restored.
   */
  def awaitQuiescent(timeout: Duration): Boolean
}

/**
 * In-flight connection accounting with signaled quiescence.
 *
 * `enter` on accept, `exit` when the connection handler returns (both exactly
 * once per connection); `awaitEmpty` blocks until the count reaches zero or the
 * timeout elapses. A `ReentrantLock` plus `Condition` wakes waiters the moment
 * the last connection exits — no poll quantum, no sleep.
 */
private[http] final class InFlightTracker {
  private val lock    = new ReentrantLock()
  private val settled = lock.newCondition()
  private var count   = 0

  def enter(): Unit = {
    lock.lock()
    try count += 1
    finally lock.unlock()
  }

  def exit(): Unit = {
    lock.lock()
    try {
      count -= 1
      if (count == 0) settled.signalAll()
    } finally lock.unlock()
  }

  /**
   * Block until the count reaches zero or `timeout` elapses. Returns false on
   * timeout or interrupt (restoring the interrupt flag); never throws.
   */
  def awaitEmpty(timeout: Duration): Boolean = {
    val deadline = System.nanoTime() + timeout.toNanos
    lock.lock()
    try {
      while (count > 0) {
        val remaining = deadline - System.nanoTime()
        if (remaining <= 0L) return false
        try settled.await(remaining, TimeUnit.NANOSECONDS)
        catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            return false
        }
      }
      true
    } finally lock.unlock()
  }
}

/**
 * One bound protocol engine managed by an [[AggregateServerHandle]].
 *
 * Adapts a thick [[ProtocolEngine]] plus the listener binding its `start`
 * returned to the [[LifecycleEngine]] shutdown vocabulary: global stop-accept,
 * deadline-bounded concurrent drain, survivor force-close.
 */
private[http] final class BoundProtocolEngine(
  engine: ProtocolEngine,
  bound: BoundConnectorHandle,
) extends LifecycleEngine {

  /** Binding reported on the aggregate handle. */
  val binding: BoundConnector = bound.binding

  def name: String = engine.id.value

  /**
   * Stop accepting new TCP connections on this engine's listener (established
   * connections stay alive), then drain in-flight work. The drain runs even
   * when stopping the listener fails; the aggregate handle records that failure
   * without skipping the drain.
   */
  def requestStop(): Unit =
    try bound.stopAccepting0()
    finally engine.drain()

  /**
   * Wait up to `timeout` for in-flight connections to settle. Engines without
   * quiescence accounting drained synchronously in `requestStop` and report
   * done at once.
   */
  def awaitDrain(timeout: Duration): Boolean =
    engine match {
      case quiescent: QuiescentEngine => quiescent.awaitQuiescent(timeout)
      case _                          => true
    }

  /**
   * Force-close owned connections, then release the listener binding. The
   * binding is released even when the engine close throws.
   */
  def forceClose(): Unit =
    try engine.close()
    finally bound.close0()
}

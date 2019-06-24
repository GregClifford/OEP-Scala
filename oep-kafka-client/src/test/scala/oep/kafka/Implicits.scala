package oep.kafka

import java.nio.channels.AsynchronousChannelGroup
import cats.effect.{ContextShift, IO, Timer}
import scala.concurrent.ExecutionContext
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

object Implicits {

  val ex: ExecutorService = Executors.newCachedThreadPool()

  implicit def AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(ex)

  implicit def T: Timer[IO] = IO.timer(ExecutionContext.global)

  implicit def CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

}

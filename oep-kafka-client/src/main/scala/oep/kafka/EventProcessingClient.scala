package oep.kafka

import java.nio.channels.AsynchronousChannelGroup

import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.{KafkaClient, offset}
import spinoco.fs2.log.Log
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}
import retry._

import scala.concurrent.duration._

class EventProcessingClient[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect : Sleep]
(host: String,
 port: Int,
 clientName: String,
 offsetStore: OffsetStore[F])
(implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]) {

  private val retryPolicy: RetryPolicy[F] = RetryPolicies.constantDelay[F](5.seconds)

  def logError(err: Throwable, details: RetryDetails): F[Unit] = details match {

    case WillDelayAndRetry(_: FiniteDuration, retriesSoFar: Int, _: FiniteDuration) =>
      F.delay(L.error(s"Failed with error ${err.getMessage}. So far we have retried $retriesSoFar times."))

    case GivingUp(totalRetries: Int, totalDelay: FiniteDuration) =>
      F.delay(L.error(s"Failed with error ${err.getMessage}. Giving up after $totalRetries retries"))
  }

  private def createClient : Stream[F, KafkaClient[F]] = {
    Stream.eval(F.delay(L.info(s"Creating Kafka client for $host:$port")))
      .flatMap { _ => ClientFactory[F].create(host, port, clientName)}
  }

  private def createOffsetCache : Stream[F, Ref[F, Long @@ Offset]] = Stream.eval(Ref.of[F, Long @@ Offset](offset(-1)))

  private def createConsumer(client : KafkaClient[F],
                             offsetCache : Ref[F, Long @@ Offset],
                             topicName : String @@ TopicName,
                             partitionId : Int @@ PartitionId,
                             op : (ByteVector, ByteVector) => Stream[F, Unit]): Stream[F, Unit] = {
    for {
      _ <- Stream.eval(L.info(s"Consuming from topic:$topicName partition: $partitionId"))
      c = Consumer[F](client, offsetStore, offsetCache, topicName, partitionId).consume(op)
      s <- Stream.eval(retryingOnAllErrors[Unit](retryPolicy, logError)(c.compile.drain))
    } yield s
  }


  def start(topicName : String @@ TopicName, partitionId : Int @@ PartitionId, op : (ByteVector, ByteVector) => Stream[F, Unit]): Stream[F, Unit] = {
    for {
      client <- createClient
      offsetCache <- createOffsetCache
      _ <- createConsumer(client, offsetCache, topicName, partitionId, op).covary
    } yield ()
  }

  def startN(config : List[(String @@ TopicName, Int @@ PartitionId, (ByteVector, ByteVector) => Stream[F, Unit])]) : Stream[F, Unit] = {
    for {
      client <- createClient
      streams = Stream.emits(config).map{c =>
        createOffsetCache.flatMap(offsetCache => createConsumer(client, offsetCache, c._1, c._2, c._3))}.covary[F]
      _ <- streams.parJoinUnbounded
    } yield ()
  }
}

object EventProcessingClient{

  def apply[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect : Sleep](host: String,
            port: Int,
            clientName: String,
            offsetStore: OffsetStore[F])
           (implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]) : EventProcessingClient[F] = new EventProcessingClient(host, port, clientName, offsetStore)

}

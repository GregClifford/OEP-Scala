package oep.kafka

import java.nio.channels.AsynchronousChannelGroup

import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.{KafkaClient, offset}
import spinoco.fs2.log.Log
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}

import scala.concurrent.duration._

class eventProcessingClient[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect]
(host: String,
 port: Int,
 clientName: String,
 offsetStore: offsetStore[F],
 op : (ByteVector, ByteVector) => Stream[F, Unit])
(implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]) {

  private def createClient : Stream[F, KafkaClient[F]] = {
    Stream.eval(F.delay(L.info(s"Creating Kafka client for $host:$port")))
      .flatMap { _ => clientFactory[F].create(host, port, clientName)}
  }

  private def createOffsetCache : Stream[F, Ref[F, Long @@ Offset]] = Stream.eval(Ref.of[F, Long @@ Offset](offset(-1)))

  private def createConsumer(client : KafkaClient[F],
                             offsetCache : Ref[F, Long @@ Offset],
                             topicName : String @@ TopicName,
                             partitionId : Int @@ PartitionId ): Stream[F, Unit] = {
    for {
      _ <- Stream.eval(L.info(s"Consuming from topic:$topicName partition: $partitionId"))
      c = consumer[F](client, offsetStore, offsetCache, topicName, partitionId).consume(op)
      s <- Stream.retry(c.compile.drain, 1.seconds, _ => 1.seconds, 1)
    } yield s
  }


  def start(topicName : String @@ TopicName, partitionId : Int @@ PartitionId): Stream[F, Unit] = {
    for {
      client <- createClient
      offsetCache <- createOffsetCache
      _ <- createConsumer(client, offsetCache, topicName, partitionId).covary
    } yield ()
  }

  def startN(config : List[(String @@ TopicName, Int @@ PartitionId)]) : Stream[F, Unit] = {
    for {
      client <- createClient
      streams = Stream.emits(config).map{tp =>
        createOffsetCache.flatMap(offsetCache => createConsumer(client, offsetCache, tp._1, tp._2))}.covary[F]
      _ <- streams.parJoinUnbounded
    } yield ()
  }
}

object eventProcessingClient{

  def apply[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect](host: String,
            port: Int,
            clientName: String,
            offsetStore: offsetStore[F],
            op: (ByteVector, ByteVector) => Stream[F, Unit])
           (implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]) : eventProcessingClient[F] = new eventProcessingClient(host, port, clientName, offsetStore, op)

}

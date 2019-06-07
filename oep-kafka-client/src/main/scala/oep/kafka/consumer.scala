package oep.kafka

import cats.effect.{Concurrent, Effect, Timer}
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.KafkaClient
import spinoco.protocol.kafka.{PartitionId, TopicName}

import scala.concurrent.duration._

class consumer[F[_] : Effect : Timer : Concurrent](client: KafkaClient[F],
                     offsetStore: offsetStore[F],
                     topicName: String @@ TopicName,
                     partitionId: Int @@ PartitionId) {

  def consume(f : (ByteVector, ByteVector) => Stream[F, Unit]) : Stream[F, Unit] = {

    val offsetWriter = for {
      _ <- Stream.awakeEvery[F](5.second)
      offsets <- Stream.eval(client.offsetRangeFor(topicName, partitionId))
      _ <- Stream.eval(offsetStore.write(topicName, partitionId, offsets._2))
    } yield ()


    val consumer = for {
      o <- Stream.eval(offsetStore.read(topicName, partitionId))
      tm <- client.subscribe(topicName, partitionId, o)
      _ <- f(tm.key, tm.message)
    } yield ()

    //add error logging to streams, merge and compile to effect
    consumer.merge(offsetWriter)
  }
}

object consumer {

  def apply[F[_] : Effect : Timer : Concurrent](client: KafkaClient[F], offsetStore: offsetStore[F], topicName: String @@ TopicName, partitionId: Int @@ PartitionId): consumer[F] =
    new consumer(client, offsetStore, topicName, partitionId)

}

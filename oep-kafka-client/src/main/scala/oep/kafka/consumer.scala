package oep.kafka

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Effect, Timer}
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.KafkaClient
import spinoco.fs2.log.Log
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}

import scala.concurrent.duration._

class consumer[F[_] : Timer](client: KafkaClient[F],
                             offsetStore: offsetStore[F],
                             offsetCache: Ref[F, Long @@ Offset],
                             topicName: String @@ TopicName,
                             partitionId: Int @@ PartitionId)(implicit log: Log[F], C: Concurrent[F]) {

  def consume(f: (ByteVector, ByteVector) => Stream[F, Unit]): Stream[F, Unit] = {

    val offsetWriter = for {
      _ <- Stream.awakeEvery[F](5.second)
      o <- Stream.eval(offsetCache.get)
      _ <- Stream.eval(offsetStore.write(topicName, partitionId, o))
    } yield ()

    val consumer = for {
      optOff <- Stream.eval(offsetStore.read(topicName, partitionId))
      o <- optOff match {
        //if the offset can't be retrieved from the offset store start at the beginning
        case Some(o) => Stream.eval(C.pure(o))
        case None => Stream.eval(client.offsetRangeFor(topicName, partitionId)).map(_._1)
      }
      tm <- client.subscribe(topicName, partitionId, o)
      _ <- f(tm.key, tm.message)
      _ <- Stream.eval(offsetCache.set(tm.offset))
    } yield ()

    consumer.merge(offsetWriter)
      .handleErrorWith { t =>
        log.error(t.getMessage)
        Stream.raiseError(t)
      }
  }
}

object consumer {

  def apply[F[_] : Effect : Timer : Concurrent](client: KafkaClient[F], offsetStore: offsetStore[F], offsetCache: Ref[F, Long @@ Offset], topicName: String @@ TopicName, partitionId: Int @@ PartitionId)(implicit log: Log[F]): consumer[F] =
    new consumer(client, offsetStore, offsetCache, topicName, partitionId)

}

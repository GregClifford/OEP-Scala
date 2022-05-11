package oep.kafka

import cats.Applicative.ops.toAllApplicativeOps
import cats.FlatMap
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Timer}
import cats.implicits.toFlatMapOps
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.{KafkaClient, offset}
import spinoco.fs2.log.Log
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}

import scala.concurrent.duration._

class Consumer[F[_] : Timer: FlatMap](client: KafkaClient[F],
                                      offsetStore: OffsetStore[F],
                                      offsetCache: Ref[F, Long @@ Offset],
                                      topicName: String @@ TopicName,
                                      partitionId: Int @@ PartitionId)(implicit log: Log[F], C: Concurrent[F]) {

  def consume(f: (ByteVector, ByteVector) => Stream[F, Unit]): Stream[F, Unit] = {

    val offsetWriter = for {
      _ <- Stream.awakeEvery[F](5.second)
      c = offsetCache.get.flatMap(o => offsetStore.write(topicName, partitionId, o))
      _ <- Stream.eval(c)
    } yield ()

    val offsetReader = for {
      optOff <- offsetStore.read(topicName, partitionId)
      o <- optOff match {
        //if the offset can't be retrieved from the offset store start at the beginning
        case Some(o) => C.pure(offset(o + 1))
        case None => log.info(s"No offset found, retrieving from broker..")
          .flatMap(_ => client.offsetRangeFor(topicName, partitionId)).map(_._1)
      }
      _ <- log.info(s"Starting consumer topic: $topicName partition: $partitionId at offset $o")
    } yield o

    val consumer = for {
      o <- Stream.eval(offsetReader)
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

object Consumer {

  def apply[F[_] : Timer : Concurrent: FlatMap](client: KafkaClient[F], offsetStore: OffsetStore[F], offsetCache: Ref[F, Long @@ Offset], topicName: String @@ TopicName, partitionId: Int @@ PartitionId)(implicit log: Log[F]): Consumer[F] =
    new Consumer(client, offsetStore, offsetCache, topicName, partitionId)

}

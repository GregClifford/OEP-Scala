package oep.kafka

import cats.effect.IO
import shapeless.tag.@@
import spinoco.fs2.kafka.offset
import spinoco.protocol.kafka

import scala.collection.mutable

class InMemoryOffsetStore extends OffsetStore[IO]{

  private val store = mutable.Map.empty[String, Long]

  override def write(topic: String, partitionId: Int, offset: Long @@ kafka.Offset): IO[Unit] =
    IO(store -= s"$topic-$partitionId").map(s => s += (s"$topic-$partitionId" -> offset))

  override def read(topic: String, partitionId: Int): IO[Option[Long @@ kafka.Offset]] =
    IO(store.withDefaultValue(-1)(s"$topic-$partitionId")).map(o => Some(offset(o)))
}

object InMemoryOffsetStore {
  def apply() : InMemoryOffsetStore = new InMemoryOffsetStore()
}
package oep.kafka.offsetStoreImpl

import java.io._
import cats.effect.Effect
import cats.implicits._
import oep.kafka.OffsetStore
import shapeless.tag.@@
import spinoco.fs2.kafka.offset
import spinoco.fs2.log.Log
import spinoco.protocol.kafka

import scala.io.Source

class FileOffsetStore[F[_]](dir : String)(implicit F : Effect[F], L : Log[F] ) extends OffsetStore[F] {

  private def getFilename(topic: String, partitionId: Int): String  = s"$dir/$topic-$partitionId.txt"

  override def write(topic: String, partitionId: Int, offset: Long @@ kafka.Offset): F[Unit] = {
    F.delay{
      val file = new File(getFilename(topic, partitionId))

      if(!file.exists())
        file.createNewFile()

      val bw = new BufferedWriter(new FileWriter(file))
      try {
        bw.write(offset.toString)
      }
      finally {
        bw.close()
      }
    }
  }

  override def read(topic: String, partitionId: Int): F[Option[Long @@ kafka.Offset]] = {
    F.attempt {
      F.delay(Source.fromFile(getFilename(topic, partitionId)))
          .flatMap(source => F.delay(source.getLines()).flatMap{ lines =>
            F.delay(lines.mkString.toLong).flatMap(offset => F.delay(source.close()).map(_ => offset))
          })
    }.flatMap{
      case Right(off) => F.pure(Some(offset(off)))
      case Left(t) => F.delay(L.error(t.getMessage)).map(_ => Option.empty[Long @@ kafka.Offset])
    }
  }
}
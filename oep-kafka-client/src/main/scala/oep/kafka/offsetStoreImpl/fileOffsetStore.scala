package oep.kafka.offsetStoreImpl

import java.io._

import cats.effect.Effect
import cats.implicits._
import oep.kafka.offsetStore
import shapeless.tag.@@
import spinoco.fs2.kafka.offset
import spinoco.fs2.log.Log
import spinoco.protocol.kafka

import scala.io.Source

class fileOffsetStore[F[_]](fileName : String)(implicit F : Effect[F], L : Log[F] ) extends offsetStore[F] {

  override def write(topic: String, partitionId: Int, offset: Long @@ kafka.Offset): F[Unit] = {
    F.delay{
      val file = new File(fileName)
      val bw = new BufferedWriter(new FileWriter(file))
      try{
        bw.write(offset.toString)
      }
      finally{
        bw.close()
      }
    }
  }

  override def read(topic: String, partitionId: Int): F[Option[Long @@ kafka.Offset]] = {
    val source = Source.fromFile(fileName)
    F.attempt {
      F.delay(offset(source.getLines().mkString.toLong))
    }.flatMap{

      case Right(off) => F.attempt(F.delay(source.close())).map(_ => Some(offset(off)))
      case Left(t) => F.delay(L.error(t.getMessage))
        .flatMap(_ => F.attempt(F.delay(source.close())))
        .map(_ => Option.empty[Long @@ kafka.Offset])

    }
  }
}

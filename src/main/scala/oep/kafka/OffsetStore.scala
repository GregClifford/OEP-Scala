package oep.kafka

import shapeless.tag.@@
import spinoco.protocol.kafka.Offset

trait OffsetStore[F[_]] {

  def write(topic : String, partitionId : Int, offset : Long @@ Offset) : F[Unit]
  def read(topic : String, partitionId : Int) : F[Option[Long @@ Offset]]

}


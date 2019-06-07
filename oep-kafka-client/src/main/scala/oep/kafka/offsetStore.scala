package oep.kafka

import shapeless.tag.@@
import spinoco.protocol.kafka.Offset

trait offsetStore[F[_]] {

  def write(topic : String, partitionId : Int, offset : Long @@ Offset) : F[Unit]
  def read(topic : String, partitionId : Int) : F[Long @@ Offset]

}


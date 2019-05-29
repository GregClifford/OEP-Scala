package oep.kafka

import cats.effect.Effect
import scodec.bits.ByteVector
import spinoco.fs2.kafka.{KafkaClient, _}

import scala.concurrent.duration._

class producer[F[_]](client : KafkaClient[F], topicName : String, partitionId : Int)(implicit F: Effect[F]) {

  def write(key : ByteVector, message : ByteVector): F[Long] = {

    client.publish1(topic(topicName), partition(partitionId), key, message, requireQuorum = true, 10.seconds)

  }
}

object producer {

  def apply[F[_]](client: KafkaClient[F], topicName: String, partitionId: Int)(implicit F: Effect[F]): producer[F] =
    new producer(client, topicName, partitionId)

}
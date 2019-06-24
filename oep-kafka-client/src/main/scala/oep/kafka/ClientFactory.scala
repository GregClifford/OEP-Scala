package oep.kafka

import java.nio.channels.AsynchronousChannelGroup
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import spinoco.fs2.kafka.KafkaClient
import spinoco.fs2.kafka
import spinoco.fs2.kafka._
import spinoco.protocol.kafka._
import fs2.Stream
import spinoco.fs2.log.Log

class ClientFactory[F[_]](implicit F: ConcurrentEffect[F], L : Log[F], T : Timer[F], CS : ContextShift[F], AG : AsynchronousChannelGroup) {

  def create(brokerName : String, port : Int, clientName : String): Stream[F, KafkaClient[F]] = kafka.client[F](
    ensemble = Set(broker(brokerName, port))
    , protocol = ProtocolVersion.Kafka_0_10_2
    , clientName = clientName)
}

object ClientFactory  {

  def apply[F[_]](implicit F: ConcurrentEffect[F], L : Log[F], T : Timer[F], CS : ContextShift[F], AG : AsynchronousChannelGroup): ClientFactory[F] =
    new ClientFactory[F]
}

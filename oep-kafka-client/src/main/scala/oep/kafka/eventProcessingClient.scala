package oep.kafka

import java.nio.channels.AsynchronousChannelGroup
import cats.effect._
import fs2.Stream
import scodec.bits.ByteVector
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log.spi.LoggerProvider
import spinoco.fs2.log.{Log, StandardProviders}
import scala.concurrent.duration._

class eventProcessingClient[F[_] : Concurrent : LoggerProvider : Timer : ContextShift : ConcurrentEffect]
(host: String,
 port: Int,
 clientName: String,
 offsetStore: offsetStore[F],
 op : (ByteVector, ByteVector) => Stream[F, Unit])
(implicit F: Effect[F], AG: AsynchronousChannelGroup) {

  def start(topicName : String, partitionId : Int): Stream[F, Unit] = {

    Stream.resource(StandardProviders.juliProvider[F])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[F])
          .flatMap { implicit log =>
            clientFactory[F].create(host, port, clientName)
              .flatMap { client =>
                //consume
                val c = consumer[F](client, offsetStore, topic(topicName), partition(partitionId)).consume(op)
                Stream.retry(c.compile.drain, 1.seconds, _ => 1.seconds, 1)
              }
          }
      }
  }
}

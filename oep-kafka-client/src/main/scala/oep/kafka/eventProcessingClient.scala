package oep.kafka

import java.nio.channels.AsynchronousChannelGroup

import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log.Log
import spinoco.protocol.kafka.Offset
import spinoco.fs2.kafka.offset
import scala.concurrent.duration._

class eventProcessingClient[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect]
(host: String,
 port: Int,
 clientName: String,
 offsetStore: offsetStore[F],
 op : (ByteVector, ByteVector) => Stream[F, Unit])
(implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]) {

  def start(topicName : String, partitionId : Int): Stream[F, Unit] = {

    Stream.eval(F.delay(L.info(s"Creating Kafka client for $host:$port")))
        .flatMap { _ =>
          Stream.eval(Ref.of[F, Long @@ Offset](offset(0)))
            .flatMap { offsetCache =>
              clientFactory[F].create(host, port, clientName)
                .flatMap { client =>
                  L.info(s"Consuming from topic:$topicName partition: $partitionId")
                  val c = consumer[F](client, offsetStore, offsetCache, topic(topicName), partition(partitionId)).consume(op)
                  Stream.retry(c.compile.drain, 1.seconds, _ => 1.seconds, 1)
                }
            }
        }
      }
}

/*

    Stream.resource(StandardProviders.juliProvider[F])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[F])
          .flatMap { implicit log =>
*
*/


object eventProcessingClient{

  def apply[F[_] : Concurrent : Timer : ContextShift : ConcurrentEffect](host: String,
            port: Int,
            clientName: String,
            offsetStore: offsetStore[F],
            op: (ByteVector, ByteVector) => Stream[F, Unit])
           (implicit F: Effect[F], AG: AsynchronousChannelGroup, L : Log[F]): eventProcessingClient[F] = new eventProcessingClient(host, port, clientName, offsetStore, op)

}

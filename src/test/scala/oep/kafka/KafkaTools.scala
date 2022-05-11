package oep.kafka

import cats.effect.IO
import fs2.Stream
import oep.kafka.Implicits._
import scodec.bits.ByteVector
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log.{Log, StandardProviders}

import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessLogger}

object KafkaTools {

  def publish(host : String, port : Int, topicName : String, partitionId : Int, messages : Map[String, String]): IO[List[Long]] = {

    Stream.resource(StandardProviders.juliProvider[IO])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[IO])
          .flatMap { implicit log =>
            ClientFactory[IO].create(host,port,"my-client")
              .flatMap { client =>
                Stream.emits(messages.toList)
                    .flatMap{ message =>
                      Stream.eval(client.publish1(topic(topicName),
                        partition(partitionId), ByteVector.encodeUtf8(message._1).toOption.get,
                        ByteVector.encodeUtf8(message._2).toOption.get,
                        requireQuorum = true,
                        10.seconds))
                    }
              }
          }
      }.compile.toList
  }
}



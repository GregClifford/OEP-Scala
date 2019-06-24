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

  private def createTopicCommand(topic: String) = s"kafka-topics --bootstrap-server localhost:9092 --create --topic $topic --partitions 1 --replication-factor 1"

  private def clearTopicCommand(topic: String) = s"kafka-configs --zookeeper localhost:2181 --alter --entity-name $topic --entity-type topics --add-config retention.ms=1"

  private def deleteTopicCommand(topic: String) = s"kafka-topics --bootstrap-server localhost:9092 --delete --topic $topic"

  def createTopic(topic: String): IO[Unit] = IO(Process(createTopicCommand(topic)).!!(ProcessLogger(s => System.out.println(s))))

  def clearTopic(topic: String): IO[Unit] = IO(Process(clearTopicCommand(topic)).!!(ProcessLogger(s => System.out.println(s))))

  def deleteTopic(topic: String): IO[Unit] = IO(Process(deleteTopicCommand(topic)).!!(ProcessLogger(s => System.out.println(s))))

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



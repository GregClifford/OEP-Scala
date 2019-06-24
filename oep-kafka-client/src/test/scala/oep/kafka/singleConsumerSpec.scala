package oep.kafka

import java.time.LocalDateTime

import cats.effect.IO
import fs2.Stream
import oep.kafka.offsetStoreImpl.fileOffsetStore
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import scodec.bits.ByteVector
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log.{Log, StandardProviders}
import Implicits._
import scala.collection.mutable.{Map => MMap}
import scala.concurrent.duration._

class singleConsumerSpec extends Specification with BeforeAfterAll {
  //sequential

  val topicName = s"test-topic-${LocalDateTime.now().toString.replace(":", "")}"
  val host = "127.0.0.1"
  val port = 9092
  val messages = Map("key21" -> "data1", "key22" -> "data2", "key23" -> "data3")

  override def beforeAll(): Unit = {
    val program = for {
      _ <- kafkaTools.createTopic(topicName)
      _ <- kafkaTools.publish(host, port, topicName, 0, messages)
    } yield ()
    program.unsafeRunSync()
    ()
  }

  override def afterAll(): Unit = {
    /*for {
      _ <- kafkaTools.clearTopic(topicName)
      _ <- kafkaTools.deleteTopic(topicName)
    } yield ()*/
    ()
  }

  def retrieve(op: (ByteVector, ByteVector) => Stream[IO, Unit], offsetStore: offsetStore[IO]) = Stream.resource(StandardProviders.juliProvider[IO])
    .flatMap { implicit provider =>
      Stream.resource(Log.async[IO])
        .flatMap { implicit log =>
          val e = eventProcessingClient(host, port, "my-client", offsetStore, op)
          e.start(topic(topicName), partition(0))
        }
    }.interruptAfter(5.seconds).compile.drain

  "The single consumer" should {

    "Retrieve all messages from a topic and partition when initially started." in {

      val retrievedMessages = MMap.empty[String, String]
      val op: (ByteVector, ByteVector) => Stream[IO, Unit] = { (key, data) =>
        Stream.eval(IO.delay(retrievedMessages += (key.decodeUtf8.toOption.get -> data.decodeUtf8.toOption.get)).map(_ => ()))
      }

      retrieve(op, InMemoryOffsetStore()).unsafeRunSync()

      retrievedMessages.size must ===(messages.size)
      messages
        .toList
        .map(message => retrievedMessages.toList.contains(message))
        .exists(!_) must beFalse
    }

    "Retrieve only messages after a certain offset from a topic and partition when restarted." in {

      val retrievedMessages = MMap.empty[String, String]
      val offsetStore = InMemoryOffsetStore()
      val secondMessageGroup = Map("key31" -> "data4", "key32" -> "data5", "key33" -> "data6")
      val op: (ByteVector, ByteVector) => Stream[IO, Unit] = { (key, data) =>
        Stream.eval(IO.delay(retrievedMessages += (key.decodeUtf8.toOption.get -> data.decodeUtf8.toOption.get)).map(_ => ()))
      }

      val program = for {
        //get initial messages and shutdown
        _ <- retrieve((_, _) => Stream.eval(IO.unit), offsetStore)
        //publish some more
        _ <- kafkaTools.publish(host, port, topicName, 0, secondMessageGroup)
        //retrieve from stored offset
        _ <- retrieve(op, offsetStore)
      } yield ()
      program.unsafeRunSync()
      retrievedMessages.size must ===(secondMessageGroup.size)
      secondMessageGroup
        .toList
        .map(message => retrievedMessages.toList.contains(message))
        .exists(!_) must beFalse
    }
  }
}



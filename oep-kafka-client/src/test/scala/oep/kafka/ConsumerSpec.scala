package oep.kafka

import java.time.LocalDateTime

import cats.effect.IO
import fs2.Stream
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log.{Log, StandardProviders}
import spinoco.protocol.kafka.{PartitionId, TopicName}
import Implicits._
import scala.concurrent.duration._
import scala.collection.mutable.{Map => MMap}
import retry.CatsEffect.sleepUsingTimer

class ConsumerSpec extends Specification with BeforeAfterAll {
  sequential

  val host = "127.0.0.1"
  val port = 9092

  //single consumer
  val topicName = s"test-topic-${LocalDateTime.now().toString.replace(":", "")}"
  val messages = Map("key21" -> "data1", "key22" -> "data2", "key23" -> "data3")

  //multiple consumer
  val topic1Name = s"test-topic1-${LocalDateTime.now().toString.replace(":", "")}"
  val topic2Name = s"test-topic2-${LocalDateTime.now().toString.replace(":", "")}"
  val messagesTopic1 = Map("key41" -> "data7", "key42" -> "data8", "key43" -> "data9")
  val messagesTopic2 = Map("key51" -> "data11", "key62" -> "data12", "key73" -> "data13")

  override def beforeAll(): Unit = {
    val program = for {
      _ <- DockerTools.clearKafkaContainer
      _ <- DockerTools.startKafkaContainer
      _ <- IO.sleep(30.seconds)
      _ <- KafkaTools.createTopic(topic1Name)
      _ <- KafkaTools.createTopic(topic2Name)
      _ <- KafkaTools.createTopic(topicName)
      _ <- IO.sleep(10.seconds)
      _ <- KafkaTools.publish(host, port, topic1Name, 0, messagesTopic1)
      _ <- KafkaTools.publish(host, port, topic2Name, 0, messagesTopic2)
      _ <- KafkaTools.publish(host, port, topicName, 0, messages)
    } yield ()
    program.unsafeRunSync()
    ()
  }

  override def afterAll(): Unit = {
    DockerTools.stopKafkaContainer.unsafeRunSync()
  }

  private def retrieve(op: (ByteVector, ByteVector) => Stream[IO, Unit], offsetStore: OffsetStore[IO]): IO[Unit] = Stream.resource(StandardProviders.juliProvider[IO])
    .flatMap { implicit provider =>
      Stream.resource(Log.async[IO])
        .flatMap { implicit log =>
          val e = EventProcessingClient(host, port, "my-client", offsetStore)
          e.start(topic(topicName), partition(0), op)
        }
    }.interruptAfter(5.seconds).compile.drain

  private def retrieve(config : List[(String @@ TopicName, Int @@ PartitionId, (ByteVector, ByteVector) => Stream[IO, Unit])], offsetStore: OffsetStore[IO]): IO[Unit] =
    Stream.resource(StandardProviders.juliProvider[IO])
    .flatMap { implicit provider =>
      Stream.resource(Log.async[IO])
        .flatMap { implicit log =>
          val e = EventProcessingClient(host, port, "my-client", offsetStore)
          e.startN(config)
        }
    }.interruptAfter(5.seconds).compile.drain

  "The multiple consumer" should {

    "Retrieve all messages from 2 topics and the correct partition when initially started." in {
      val retrievedMessages = MMap.empty[String, String]
      val op: (ByteVector, ByteVector) => Stream[IO, Unit] = { (key, data) =>
        Stream.eval(IO.delay(retrievedMessages += (key.decodeUtf8.toOption.get -> data.decodeUtf8.toOption.get)).map(_ => ()))
      }

      retrieve(List((topic(topic1Name), partition(0), op),(topic(topic2Name), partition(0), op)), InMemoryOffsetStore()).unsafeRunSync()

      retrievedMessages.size must ===(messagesTopic1.size + messagesTopic2.size)
      (messagesTopic1.toList ::: messagesTopic2.toList)
        .map(message => retrievedMessages.toList.contains(message))
        .exists(!_) must beFalse
    }


    "Retrieve only messages after a certain offset from a topic and partition when restarted." in {
      val retrievedMessages = MMap.empty[String, String]
      val offsetStore = InMemoryOffsetStore()
      val secondMessageGroup1 = Map("key881" -> "data456", "key992" -> "data889", "key993" -> "data768")
      val secondMessageGroup2 = Map("key1111" -> "data4123", "key2222" -> "data9945", "key3333" -> "data55678")
      val op: (ByteVector, ByteVector) => Stream[IO, Unit] = { (key, data) =>
        Stream.eval(IO.delay(retrievedMessages += (key.decodeUtf8.toOption.get -> data.decodeUtf8.toOption.get)).map(_ => ()))
      }
      val opDoNothing = (_ : ByteVector, _ : ByteVector) => Stream.eval(IO.unit)

      val program = for {
        //get initial messages and shutdown
        _ <- retrieve(List((topic(topic1Name), partition(0), opDoNothing),(topic(topic2Name), partition(0), opDoNothing)), offsetStore)
        //publish some more
        _ <- KafkaTools.publish(host, port, topic1Name, 0, secondMessageGroup1)
        _ <- KafkaTools.publish(host, port, topic2Name, 0, secondMessageGroup2)
        //retrieve from stored offset
        _ <- retrieve(List((topic(topic1Name), partition(0), op),(topic(topic2Name), partition(0), op)), offsetStore)
      } yield ()
      program.unsafeRunSync()

      retrievedMessages.size must ===(secondMessageGroup1.size + secondMessageGroup2.size)
      (secondMessageGroup1.toList ::: secondMessageGroup2.toList)
        .map(message => retrievedMessages.toList.contains(message))
        .exists(!_) must beFalse
    }
  }

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
        _ <- KafkaTools.publish(host, port, topicName, 0, secondMessageGroup)
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

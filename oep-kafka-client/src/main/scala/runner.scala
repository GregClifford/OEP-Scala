import java.nio.channels.AsynchronousChannelGroup

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import oep.kafka.eventProcessingClient
import oep.kafka.offsetStoreImpl.fileOffsetStore
import scodec.bits.ByteVector
import spinoco.fs2.kafka.{partition, topic}
import spinoco.fs2.log._

object runner extends IOApp {

  import java.util.concurrent.{ExecutorService, Executors}

  val ex: ExecutorService = Executors.newCachedThreadPool()
  implicit def AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(ex)

  override def run(args: List[String]): IO[ExitCode] = {

    Stream.resource(StandardProviders.juliProvider[IO])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[IO])
          .flatMap { implicit log =>
              val o = new fileOffsetStore[IO](s"${new java.io.File(".").getCanonicalPath}")
              val op = (key : ByteVector, data : ByteVector) => Stream.eval(IO.delay(System.out.println(s"${key.decodeUtf8.toOption.get} : ${data.decodeUtf8.toOption.get}")))
              val e = eventProcessingClient("127.0.0.1",9092,"my-client", o, op)
              e.startN(List((topic("test-topic2"), partition(0)), (topic("test-topic2"), partition(1))))
          }}.compile.drain.map(_ => ExitCode.Success)
  }
}

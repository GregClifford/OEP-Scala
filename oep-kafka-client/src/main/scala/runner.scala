import java.nio.channels.AsynchronousChannelGroup

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import oep.kafka.eventProcessingClient
import oep.kafka.offsetStoreImpl.fileOffsetStore
import scodec.bits.ByteVector
import spinoco.fs2.log._

object runner extends IOApp {

  import java.util.concurrent.{ExecutorService, Executors}

  val ex: ExecutorService = Executors.newCachedThreadPool()
  implicit def AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(ex)
  //implicit val timer = IO.timer(scala.concurrent.ExecutionContext.Implicits.global)

  override def run(args: List[String]): IO[ExitCode] = {

    Stream.resource(StandardProviders.juliProvider[IO])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[IO])
          .flatMap { implicit log =>
              val o = new fileOffsetStore[IO]("/Users/GregC/Development/OEP-Scala/oep-kafka-client/offset.txt")
              val op = (key : ByteVector, data : ByteVector) => Stream.eval(IO.delay(System.out.println(s"${key.decodeUtf8.toOption.get} : ${data.decodeUtf8.toOption.get}")))
              val e = eventProcessingClient("127.0.0.1",9092,"my-client", o, op)
              e.start("test-topic", 0)
          }}.compile.drain.map(_ => ExitCode.Success)
  }
}

import java.nio.channels.AsynchronousChannelGroup

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import oep.kafka.{clientFactory, producer}
import scodec.bits.ByteVector
import spinoco.fs2.log._

object runner extends IOApp {

  import java.util.concurrent.ExecutorService
  import java.util.concurrent.Executors

  val ex: ExecutorService = Executors.newCachedThreadPool()
  implicit def AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(ex)

  override def run(args: List[String]): IO[ExitCode] = {

    val key = args.head
    val message = args.last

    Stream.resource(StandardProviders.juliProvider[IO])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[IO])
          .flatMap { implicit log =>
            clientFactory[IO].create("local",9092,"my-client")
              .flatMap { client =>
                Stream.eval(producer[IO](client, "test-topic", 0)
                    .write(ByteVector.encodeUtf8(key).toOption.get, ByteVector.encodeUtf8(message).toOption.get)
                    .map(_ => ExitCode.Success))
              }
          }
      }.compile.toList.map(l => l.head)
  }
}

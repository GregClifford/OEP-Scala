import java.nio.channels.AsynchronousChannelGroup

import cats.effect.{Concurrent, ExitCode, IO, IOApp}
import fs2.Stream
import oep.kafka.{clientFactory, consumer, offsetStore}
import scodec.bits.ByteVector
import shapeless.tag.@@
import spinoco.fs2.log._
import spinoco.fs2.kafka._
import spinoco.protocol.kafka.{Offset, PartitionId, TopicName}
import scala.concurrent.duration._

object runner extends IOApp {

  import java.util.concurrent.ExecutorService
  import java.util.concurrent.Executors

  val ex: ExecutorService = Executors.newCachedThreadPool()
  implicit def AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(ex)
  //implicit val timer = IO.timer(scala.concurrent.ExecutionContext.Implicits.global)

  override def run(args: List[String]): IO[ExitCode] = {

    val key = args.head
    val message = args.last

    Stream.resource(StandardProviders.juliProvider[IO])
      .flatMap { implicit provider =>
        Stream.resource(Log.async[IO])
          .flatMap { implicit log =>
            clientFactory[IO].create("127.0.0.1",9092,"my-client")
              .flatMap { client =>

                val o = new offsetStore[IO] {
                  override def write(topic: String, partitionId: Int, offset: Long @@ Offset): IO[Unit] = ???

                  override def read(topic: String, partitionId: Int): IO[Long @@ Offset] = ???
                }

                //consume
                val c = consumer[IO](client,o,  topic(""), partition(0)).consume((_, _) => Stream.eval(IO.unit))

                Stream.retry(c.compile.drain , 1.seconds, _ => 1.seconds, 1)

                Stream.eval(IO.pure(ExitCode.Success))
              }
          }
      }.compile.toList.map(l => l.head)
  }
}

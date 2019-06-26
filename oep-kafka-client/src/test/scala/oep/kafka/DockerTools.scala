package oep.kafka

import cats.effect.IO
import fs2.concurrent.Queue
import oep.kafka.Implicits._

import scala.sys.process.{Process, ProcessLogger}

object DockerTools {

  val dockerComposeFile = s"${new java.io.File(".").getCanonicalPath}/src/test/resources/docker/docker-compose.yml"

  val clearKafkaContainer : IO[Unit] = IO(Process("docker-compose", Seq("-f", dockerComposeFile, "rm", "--force")).!!(ProcessLogger(s => System.out.println(s))))
  val stopKafkaContainer : IO[Unit] = IO(Process("docker-compose", Seq("-f", dockerComposeFile, "stop")).!!(ProcessLogger(s => System.out.println(s))))

  val startKafkaContainer : IO[Unit] = acquireProcess("docker-compose", Seq("-f", dockerComposeFile, "up"), "creating topics: test:1:1")
    .map(_ => ())

  private def acquireProcess(command : String, args : Seq[String], continuationLogEvent : String ) : IO[Process] = {

    val commandAndArgs = s"$command ${args.mkString(" ")}"

    for {
      stdOut <- Queue.unbounded[IO, String]
      stdErr <- Queue.unbounded[IO, String]
      cmd <- IO {
        System.out.println(s"Starting $commandAndArgs")
        Process(command, args)
      }
      process <- IO {
        cmd.run(new ProcessLogger {
          def out(s: => String): Unit = {
            System.out.println("OUT : " + s)
            stdOut.enqueue1(s).unsafeRunSync()
          }

          def err(s: => String): Unit = {
            System.out.println("ERR: " + s)
            stdErr.enqueue1(s).unsafeRunSync()
          }

          def buffer[T](f: => T): T = f
        })
      }
      _ <- stdOut.dequeue.takeThrough(!_.contains(continuationLogEvent)).compile.toList
      _ <- IO {
        System.out.println(s"$commandAndArgs +  initialised")
      }
    } yield process
  }

}

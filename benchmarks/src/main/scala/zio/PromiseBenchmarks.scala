package zio

import cats.effect.kernel.Deferred
import cats.effect.unsafe.implicits.global
import cats.effect.{IO => CIO}
import org.openjdk.jmh.annotations.{Scope => JScope, _}
import zio.BenchmarkUtil._

import java.util.concurrent.TimeUnit

@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(value = 3)
class PromiseBenchmarks {

  val n = 100000
  val waiters: Int = 16

  @Benchmark
  def zioPromiseAwaitDone(): Unit = {

    val io =
      Promise.make[Nothing, Unit].flatMap { promise =>
        promise.succeed(()) *> promise.await
      }.repeatN(n)

    unsafeRun(io)
  }

  @Benchmark
  def catsPromiseAwaitDone(): Unit = {

    val io =
      Deferred[CIO, Unit].flatMap { promise =>
        promise.complete(()).flatMap(_ => promise.get)
      }.replicateA_(waiters)

    io.unsafeRunSync()
  }

  @Benchmark
  def zioPromiseMultiAwaitDone(): Unit = {
    def loop(n: Int, promise: Promise[Nothing, Unit]): ZIO[Any, Nothing, Boolean] = {
      if (n <= 0) promise.succeed(())
      else promise.await.fork *> loop(n - 1, promise)
    }

    val io = Promise.make[Nothing, Unit].flatMap { promise =>
      loop(waiters, promise) *> promise.await
    }

    unsafeRun(io)
  }

  @Benchmark
  def catsPromiseMultiAwaitDone(): Unit = {
    def loop(n: Int, promise: Deferred[CIO, Unit]): CIO[Boolean] = {
      if (n <= 0) promise.complete(())
      else promise.get.start *> loop(n - 1, promise)
    }

    val io =
      Deferred[CIO, Unit].flatMap { promise =>
        loop(waiters, promise) *> promise.get
      }

    io.unsafeRunSync()
  }
}

package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import com.codahale.metrics._
import com.github.dockerjava.core.DockerClientBuilder

object Metrics {
  val metrics = new MetricRegistry
}

object Runner {

  implicit val docker = DockerClientBuilder.getInstance().build()

  def main(args:Array[String]) {
    val reporter = ConsoleReporter.forRegistry(Metrics.metrics)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build()
    reporter.start(1, TimeUnit.SECONDS)

    val SERVERS = 1000
    val LIVEDASHBOARDS = 5

    val system = ActorSystem("park")
    try {
      // val store = GraphiteStore("192.168.59.103", 2003, 80)
      val store = InfluxDBStore("192.168.59.103", 8086, "test", "test", "test")
      (0 until SERVERS).foreach { i => system.actorOf(CollectorAgent.props(store), "server-%06d".format(i)) }
      (0 until LIVEDASHBOARDS).foreach { i => system.actorOf(DashboardingAgent.props(store,
        (0 to 8).map( i => "m" + scala.util.Random.nextInt(10))
      ), "live-%06d".format(i)) }
      System.in.read
    } finally {
      system.shutdown
      reporter.stop()
    }
  }
}

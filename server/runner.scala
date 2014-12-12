package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import com.codahale.metrics._
import com.github.dockerjava.core.DockerClientBuilder

object Environment {
  val metrics = new MetricRegistry
  val dockerHost = Option(System.getenv().get("DOCKER_HOST"))
    .getOrElse("tcp://192.168.59.103:2376").drop(6).dropRight(5)
  val docker = DockerClientBuilder.getInstance().build()
  val df = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
  df.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  new java.io.File("target/metrics").mkdirs()
  val reporter = CsvReporter.forRegistry(Environment.metrics)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build(new java.io.File("target/metrics/"+df.format(new Date())+".csv"))

  reporter.start(1, TimeUnit.SECONDS)
  def close() {
    reporter.stop()
  }
}

object Runner {

  def main(args:Array[String]) {

    val SERVERS = 2
    val LIVEDASHBOARDS = 5

    val system = ActorSystem("park")
    //val store = GraphiteStore("192.168.59.103", 2003, 80)
    val store = InfluxDBStore()
    try {
      store.startContainer
      (0 until SERVERS).foreach { i => system.actorOf(CollectorAgent.props(store), "server-%06d".format(i)) }
      (0 until LIVEDASHBOARDS).foreach { i => system.actorOf(DashboardingAgent.props(store,
        (0 to 8).map( i => "m" + scala.util.Random.nextInt(10))
      ), "live-%06d".format(i)) }
      System.in.read
    } finally {
      system.shutdown
      store.stopContainer
      Environment.close
    }
  }
}

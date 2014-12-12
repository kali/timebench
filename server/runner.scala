package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import com.codahale.metrics._
import com.github.dockerjava.core.DockerClientBuilder

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

object Environment {
  val lc = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]

  val metrics = new MetricRegistry
  val dockerHost = Option(System.getenv().get("DOCKER_HOST"))
    .getOrElse("tcp://192.168.59.103:2376").drop(6).dropRight(5)
  val docker = DockerClientBuilder.getInstance().build()
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
  dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  val csvDir = new java.io.File("target/metrics/"+dateFormat.format(new Date()))
  csvDir.mkdirs()
  val reporter = CsvReporter.forRegistry(Environment.metrics)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build(csvDir)

  reporter.start(1, TimeUnit.SECONDS)
  def close() {
    reporter.stop()
  }
}

object Runner {

  val logger = org.slf4j.LoggerFactory.getLogger("bench")

  def main(args:Array[String]) {
    org.slf4j.bridge.SLF4JBridgeHandler.removeHandlersForRootLogger()
    org.slf4j.bridge.SLF4JBridgeHandler.install()
    val store = InfluxDBStore()
    //val store = NotAStore
    List(InfluxDBStore(), GraphiteStore(), NotAStore).foreach { store =>
      store.startContainer
      logger.info { "%20s\t%d".format(store.containerName, duFor1week1server(store)) }
      store.stopContainer
    }
  }

  // 1 machine, 1 week of data
  def duFor1week1server(store:StoreInterface):Long = {
    val oneWeekAgo:Long = ((System.currentTimeMillis - (7 day).toMillis) / (1 day).toMillis).toLong * (1 day).toMillis
    val yesterday:Long = oneWeekAgo + (7 days).toMillis
    feed(store, oneWeekAgo, yesterday, 1)
    store.diskUsage
  }

  def feed(store:StoreInterface, from:Long, to:Long, n:Int) {
    (from to to by (10 seconds).toMillis).foreach { ts =>
        Retry(1, 5 seconds) { () =>
          CollectorAgent.collect(store, "duByTime", new Date(ts))
      }
    }
  }

  def load(args:Array[String]) {

    val SERVERS = 2
    val LIVEDASHBOARDS = 5

    val system = ActorSystem("park")
//val store = GraphiteStore()
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

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
  val csvDir = new java.io.File("target/metrics/")
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
    val store = InfluxDBStore
    //val store = NotAStore
    List(
      InfluxDBStore,
      IndexedMongoDBStore,
      GraphiteStore,
      NotAStore
    ).foreach { store =>
      store.startContainer
      liveDashboardBench(store, 1, false)
      store.stopContainer
    }
  }

  def duFor1week1server(store:StoreInterface):Long = {
    val yesterday:Long = oneWeekAgo + (7 days).toMillis
    feed(store, oneWeekAgo, yesterday, "s")
    store.diskUsage
  }
  def oneWeekAgo:Long = ((System.currentTimeMillis - (7 day).toMillis) / (1 day).toMillis).toLong * (1 day).toMillis
  def oneDayAgo:Long = ((System.currentTimeMillis - (1 day).toMillis) / (1 day).toMillis).toLong * (1 day).toMillis
  def now:Long = System.currentTimeMillis

  def feed(store:StoreInterface, from:Long, to:Long, name:String) {
    (from until to by (10 seconds).toMillis).foreach { ts =>
        Retry(1, 5 seconds) { () =>
          CollectorAgent.collect(store, name, new Date(ts))
      }
    }
  }

  def liveDashboardBench(store:StoreInterface, servers:Int, fast:Boolean=false) {
    logger.info(s"feeding $store for dashboard bench with $servers servers")
    val system = ActorSystem("live")
    val epoch = if(fast) oneDayAgo else oneWeekAgo
    (1 to servers).foreach { i =>
      val start = System.currentTimeMillis
      feed(store, epoch, now, "server-%06d".format(i))
      val time = System.currentTimeMillis - start
      logger.info(s"fed server $i in $time ms")
      system.actorOf(CollectorAgent.props(store), "server-%06d".format(i))
    }
    DashboardingAgent.reset
    AuditingAgent.reset
    system.actorOf(DashboardingAgent.props(store, List("m3")))
    system.actorOf(AuditingAgent.props(store, epoch, List("m3")))
    Thread.sleep((if(fast) 30 seconds else 5 minutes).toMillis)
    system.shutdown
    logger.info(store + " average dashboard query time: " + DashboardingAgent.averageQueryTime + "ms, points:" + DashboardingAgent.resultCount.getCount)
    logger.info(store + " average audit query time: " + AuditingAgent.averageQueryTime + "ms, points:" + AuditingAgent.resultCount.getCount)
  }

  def load(args:Array[String]) {

    val SERVERS = 2
    val LIVEDASHBOARDS = 5

    val system = ActorSystem("park")
//val store = GraphiteStore()
    val store = InfluxDBStore
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
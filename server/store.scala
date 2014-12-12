package server

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import com.codahale.metrics._
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

object Types {
  type Server = String
  type Probe = String
  type Key = String
  type Value = Double
}
import Types._

trait StoreInterface {
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)])
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:Probe):Iterator[(Date,Server,Key,Value)]
}

object NotAStore extends StoreInterface {
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
}

case class GraphiteStore(hostname:String, portTcp:Int, portHttp:Int) extends StoreInterface {
  import java.net._
  import java.io._
  import scala.io._

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val s = new Socket(InetAddress.getByName(hostname), portTcp)
    val out = new PrintStream(s.getOutputStream())
    val date = timestamp.getTime() / 1000
    values.foreach {
      case (s,p,k,v) => out.println(s"10sec.$s.$p.$k $v $date")
    }
    s.close
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
}

case class InfluxDBStore(hostname:String, portHttp:Int, db:String, user:String, pwd:String)
    extends StoreInterface {
  import scalaj.http._
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  val request = Http(s"http://$hostname:$portHttp/db/$db/series?u=$user&p=$pwd&time_precision=s")
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val date = timestamp.getTime() / 1000
    val body = ( "name" -> "test") ~
      ("columns" -> List("time", "server", "probe", "key", "value") ) ~
      ("points" -> values.map { case (s,p,k,v) => JArray(List(date, s, p, k ,v)) } )
    val code = request.postData(compact(render(JArray(List(body))))).asBytes.code
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
}

object CollectorAgent {
  def props(store:StoreInterface):Props = Props(new CollectorAgent(store))
  val collection = Bench.metrics.counter(MetricRegistry.name(getClass(), "collection"));
  val collectionMS = Bench.metrics.counter(MetricRegistry.name(getClass(), "collectionMS"));
}
class CollectorAgent(store:StoreInterface) extends Actor {
  object Tick
  val ticker = context.system.scheduler.schedule(0 milliseconds, 10 seconds, self, Tick)
  def receive = {
    case Tick =>
      try {
        val storable = for(metric <- 0 to 9 ; key <- 0 to 9)
            yield (self.path.name, "m" + metric.toString, "k" + key.toString, Math.random())
        val before = System.currentTimeMillis
        store.storeValues(new Date(), storable)
        val after = System.currentTimeMillis
        CollectorAgent.collection.inc(1)
        CollectorAgent.collectionMS.inc(after-before)
      } catch {
        case e:Throwable => println("error: " + e)
      }
  }
}

object DashboardingAgent {
  def props(store:StoreInterface, pick:Seq[Probe]):Props = Props(new DashboardingAgent(store, pick))
}
class DashboardingAgent(store:StoreInterface, pick:Seq[Probe]) extends Actor {
  object Tick
  val ticker = context.system.scheduler.schedule(0 milliseconds, 15 seconds, self, Tick)
  def receive = {
    case Tick =>
  }
}

object Bench {
  val metrics = new MetricRegistry

  def main(args:Array[String]) {
    val reporter = ConsoleReporter.forRegistry(metrics)
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

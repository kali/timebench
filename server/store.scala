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
  val stored = Bench.metrics.counter(MetricRegistry.name(getClass(), "stored"));
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    stored.inc(values.size)
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
}

object CollectorAgent {
  def props(store:StoreInterface):Props = Props(new CollectorAgent(store))
}
class CollectorAgent(store:StoreInterface) extends Actor {
  object Tick
  val ticker = context.system.scheduler.schedule(0 milliseconds, 10 seconds, self, Tick)
  def receive = {
    case Tick =>
      store.storeValues(
        new Date(),
        for(metric <- 0 to 9 ; key <- 0 to 9)
          yield (self.path.name, "m" + metric.toString, "k" + key.toString, Math.random())
      )
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
      val store = NotAStore
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

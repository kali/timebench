package bench

import java.util.Date
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import com.codahale.metrics._

import Types._

object CollectorAgent {
  def props(store:StoreInterface):Props = Props(new CollectorAgent(store))
  val collection = Metrics.metrics.counter(MetricRegistry.name(getClass(), "collection"));
  val collectionMS = Metrics.metrics.counter(MetricRegistry.name(getClass(), "collectionMS"));
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

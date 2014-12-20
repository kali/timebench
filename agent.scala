package bench

import java.util.Date
import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import com.codahale.metrics._

import Types._

trait QueryProfiling {
  val prefix = getClass()
  val queryCount = Environment.metrics.counter(MetricRegistry.name(getClass(), "query"))
  val queryElapsed = Environment.metrics.counter(MetricRegistry.name(getClass(), "elapsed"))
  val resultCount = Environment.metrics.counter(MetricRegistry.name(getClass(), "results"))
  def profile[T](q : =>Seq[T]):Seq[T] = {
    val before = System.currentTimeMillis
    val result = q
    val after = System.currentTimeMillis
    queryCount.inc(1)
    queryElapsed.inc(after-before)
    resultCount.inc(result.size)
    result
  }
  def reset {
    queryElapsed.dec(queryElapsed.getCount)
    queryCount.dec(queryCount.getCount)
    resultCount.dec(resultCount.getCount)
  }
  def averageQueryTime = if(queryCount.getCount > 0) queryElapsed.getCount / queryCount.getCount else 999999
}

object CollectorAgent extends QueryProfiling {
  def props(store:StoreInterface):Props = Props(new CollectorAgent(store))
  def collect(store:StoreInterface, name:String, date:Date=new Date()) {
    val storable = for(metric <- 0 to 9 ; key <- 0 to 9)
        yield (name, "m" + metric.toString, "k" + key.toString, Math.random())
    CollectorAgent.profile{ store.storeValues(date, storable) ; storable }
  }
}
class CollectorAgent(store:StoreInterface) extends Actor {
  object Tick
  val ticker = context.system.scheduler.schedule(0 milliseconds, 10 seconds, self, Tick)
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def receive = {
    case Tick =>
      try {
        CollectorAgent.collect(store, self.path.name)
      } catch {
        case e:Exception => logger.warn(e.toString)
      }
  }
}

abstract class QueryAgent(store:StoreInterface, pick:Seq[Probe]) extends Actor {
  object Tick
  val ticker = context.system.scheduler.schedule(0 milliseconds, 15 seconds, self, Tick)
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def collector:QueryProfiling
  def receive = {
    case Tick => {
      try {
        pick.foreach( probe =>
          collector.profile {
            val t = time
            store.pullProbe(new Date(t), new Date(t + (15 minutes).toMillis), probe)
          }
        )
      } catch {
        case e:Exception => logger.warn(e.toString)
      }
    }
  }
  def time:Long
}

object DashboardingAgent extends QueryProfiling {
  def props(store:StoreInterface, pick:Seq[Probe]):Props = Props(new DashboardingAgent(store, pick))
}
class DashboardingAgent(store:StoreInterface, pick:Seq[Probe]) extends QueryAgent(store,pick) {
  def time:Long = System.currentTimeMillis - (15 minutes).toMillis
  def collector = DashboardingAgent
}

object AuditingAgent extends QueryProfiling {
  def props(store:StoreInterface, epoch:Long, pick:Seq[Probe]):Props = Props(new AuditingAgent(store, epoch, pick))
}
class AuditingAgent(store:StoreInterface, val epoch:Long, pick:Seq[Probe]) extends QueryAgent(store, pick) {
  def collector = AuditingAgent
  def time:Long = epoch + (scala.util.Random.nextFloat*(System.currentTimeMillis - epoch)).toLong
}

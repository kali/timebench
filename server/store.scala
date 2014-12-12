package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import com.codahale.metrics._
import java.util.concurrent.TimeUnit

import scala.sys.process._

import scala.concurrent.duration._

object Types {
  type Server = String
  type Probe = String
  type Key = String
  type Value = Double
}
import Types._

trait StoreInterface {
  def startContainer {
    stopContainer
    doStartContainer
  }
  def doStartContainer
  def stopContainer {
    try {
      Environment.docker.removeContainerCmd(containerName).withForce().withRemoveVolumes(true).exec
    } catch {
      case a:com.github.dockerjava.api.NotFoundException => {}
    }
  }
  def containerName:String = this.getClass().getSimpleName.toLowerCase

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)])
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:Probe):Iterator[(Date,Server,Key,Value)]

  def exec(cmd:String):String = s"docker exec $containerName $cmd" !!
  def diskDataPath:String
  def diskUsage:Long = exec(s"du -bs $diskDataPath").split("""[ \t]""").head.toLong
}

object NotAStore extends StoreInterface {
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
  def doStartContainer {}
  def diskDataPath:String = ""
  override def diskUsage = 0
}

object Retry {
  def apply[T](times:Int, pause:Duration)(what:(() => T)):T = {
    (0 until times).foreach { i =>
      try {
        return what()
      } catch {
        case t:Exception => {
          Thread.sleep(pause.toMillis)
        }
      }
    }
    what()
  }
}

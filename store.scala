package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import com.codahale.metrics._
import java.util.concurrent.TimeUnit

import scala.sys.process._

import scala.concurrent.duration._

import org.slf4j.Logger

object Types {
  type Server = String
  type Probe = String
  type Key = String
  type Value = Double
}
import Types._

trait StoreInterface {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def startContainer {
    stopContainer
    logger.debug("starting")
    doStartContainer
    logger.debug("started")
  }
  def doStartContainer
  def stopContainer {
    try {
      Environment.docker.removeContainerCmd(containerName).withForce().withRemoveVolumes(true).exec
    } catch {
      case a:com.github.dockerjava.api.NotFoundException => {}
    }
  }
  def containerName:String = this.getClass().getSimpleName.replaceAllLiterally("$","").toLowerCase

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)])
  def pullProbe(start:Date, stop:Date, metric:Probe):List[(Date,Server,Key,Value)]

  def exec(cmd:String):String = s"docker exec $containerName $cmd" !!
  def diskDataPath:String
  def diskUsage:Long = exec(s"du -bs $diskDataPath").split("""[ \t]""").head.toLong

  def drain(is:java.io.InputStream) {
    Iterator.continually (is.read).takeWhile(-1 !=).foreach( a => () )
  }
}

object NotAStore extends StoreInterface {
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
  }
  def pullProbe(start:Date, stop:Date, metric:String):List[(Date,Server,Key,Value)] = List()
  def doStartContainer {}
  def diskDataPath:String = ""
  override def diskUsage = 0
}

object Retry {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  def apply[T](times:Int, pause:Duration, log:Logger=logger)(what:(() => T)):T = {
    (0 until times).foreach { i =>
      try {
        return what()
      } catch {
        case t:Exception => {
          log.debug(s"$t [will retry in $pause]")
          Thread.sleep(pause.toMillis)
        }
      }
    }
    what()
  }
}

package bench

import java.net._
import java.io._
import scala.io._

import java.util.Date
import scala.concurrent.duration._

import Types._

import com.github.dockerjava.api.DockerClient

object GraphiteStore {
  val logger = org.slf4j.LoggerFactory.getLogger(classOf[GraphiteStore])
}
case class GraphiteStore() extends StoreInterface {

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val s = new Socket(InetAddress.getByName(Environment.dockerHost), 2003)
    val out = new PrintStream(s.getOutputStream())
    val date = timestamp.getTime() / 1000
    values.foreach {
      case (s,p,k,v) => out.println(s"10sec.$s.$p.$k $v $date")
    }
    s.close
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()

  import Environment.docker
  import com.github.dockerjava.api.model._
  def doStartContainer {
    val image = "timebench-graphite"
    val is = docker.buildImageCmd(new java.io.File("docker/graphite")).withTag(image).exec()
    Iterator.continually (is.read).takeWhile(-1 !=).foreach ( GraphiteStore.logger.debug("%s", _) )
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(2003))
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("2003:2003")).exec()
    Retry(103, 10 seconds) { () =>
      new Socket(InetAddress.getByName(Environment.dockerHost), 2003).close()
    }
  }

  def diskDataPath = "/opt/graphite/storage/whisper"
}

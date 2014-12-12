package bench

import java.net._
import java.io._
import scala.io._

import java.util.Date
import scala.concurrent.duration._

import Types._

import com.github.dockerjava.api.DockerClient

case class GraphiteStore(hostname:String, portTcp:Int, portHttp:Int) extends StoreInterface {
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

  import Environment.docker
  def startContainer {
    val image = "timebench-graphite"
    val is = docker.buildImageCmd(new java.io.File("docker/graphite")).withTag(image).exec()
    Iterator.continually (is.read).takeWhile(-1 !=).foreach(System.out.write)
    docker.createContainerCmd(image).withName(containerName)
      .withPortSpecs("80:80","2003:2003","8125:8125/udp")
      .exec()
    docker.startContainerCmd(containerName).exec()
    Retry(100, 10 seconds) { () =>
      new Socket(InetAddress.getByName(hostname), portTcp).close()
    }
  }

}

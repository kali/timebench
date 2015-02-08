package bench

import java.net._
import java.io._
import scala.io._

import java.util.Date
import scala.concurrent.duration._

import Types._

import com.github.dockerjava.api.DockerClient

import scalaj.http._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object GraphiteStore extends StoreInterface {
  val url = "http://" + Environment.dockerHost
  val dateFormat = SafeSimpleDateFormat("HH:mm'_'yyyyMMdd")

  val socketHolder = new java.lang.ThreadLocal[(Socket,Int)]()
  def withSocket[T](f:Socket => T) {
    if(socketHolder.get() == null || socketHolder.get()._2 == 64) {
      socketHolder.set(new Socket(InetAddress.getByName(Environment.dockerHost), 2003),0)
    }
    socketHolder.set(socketHolder.get()._1, socketHolder.get()._2 + 1)
    f(socketHolder.get()._1)
  }

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    withSocket { s =>
      println("connected ? : " + s.isConnected);
      val out = new PrintStream(s.getOutputStream())
      val date = timestamp.getTime() / 1000
      values.foreach {
        case (s,p,k,v) => { out.println(s"10sec.$s.$p.$k $v $date")
          out.flush()
        }
      }
      out.close();
    }
  }

  def pullProbe(start:Date, stop:Date, metric:String):List[(Date,Server,Key,Value)] = {
    val req = Http(url + "/render")
        .params(
          "from" -> dateFormat.format(start),
          "until" -> dateFormat.format(stop),
          "target" -> s"10sec.*.$metric.*",
          "format" -> "json")
    logger.debug("query: " + req.url)
    val resp = parse(StringInput(req.asString.body))
    val result = resp.asInstanceOf[JArray].values.flatMap {
      case obj:Map[String,_] =>
        val Array(_,server,_,key) = obj("target").toString.split('.')
        obj("datapoints").asInstanceOf[List[List[_]]].flatMap {
          case v :: ts :: Nil if(v != null) => Some((new Date(1000L*ts.asInstanceOf[BigInt].toLong), server, key, v.asInstanceOf[Double]))
          case _ => None
        }
    }
    println(result.size)
    result
  }

  import Environment.docker
  import com.github.dockerjava.api.model._
  def doStartContainer {
    val image = "timebench-graphite"
    val is = docker.buildImageCmd(new java.io.File("docker/graphite")).withTag(image).exec()
    drain(is)
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(2003), new ExposedPort(80), new ExposedPort(8080))
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("2003:2003"), PortBinding.parse("80:80"), PortBinding.parse("8080:8080")).exec()
    Retry(103, 10 seconds,logger) { () =>
      logger.debug("dockerhost is " + Environment.dockerHost)
      new Socket(InetAddress.getByName(Environment.dockerHost), 2003).close()
    }
  }

  override def stopContainer = {
    super.stopContainer
  }

  def diskDataPath = "/opt/graphite/storage/whisper"

}

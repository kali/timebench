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

import org.apache.commons.pool2._

object GraphiteStore extends StoreInterface {
  val url = "http://" + Environment.dockerHost
  val dateFormat = new java.text.SimpleDateFormat("HH:mm'_'yyyyMMdd")
  dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  val factory:PooledObjectFactory[Socket] = new PooledObjectFactory[Socket] {
    def activateObject(x:PooledObject[Socket]): Unit = {}
    def destroyObject(x:PooledObject[Socket]): Unit = x.getObject.close
    def makeObject():PooledObject[Socket] = {
      new org.apache.commons.pool2.impl.DefaultPooledObject(
        new Socket(InetAddress.getByName(Environment.dockerHost), 2003)
      )
    }
    def passivateObject(x:PooledObject[Socket]): Unit = {}
    def validateObject(x:PooledObject[Socket]): Boolean = x.getObject.isConnected
  }

  var pool:ObjectPool[Socket] = null

  def withSocket[T](f:Socket => T) {
    val s = pool.borrowObject
    try { f(s) } finally { if(s!=null) pool.returnObject(s) }
  }

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    withSocket { s =>
      val out = new PrintStream(s.getOutputStream())
      val date = timestamp.getTime() / 1000
      values.foreach {
        case (s,p,k,v) => out.println(s"10sec.$s.$p.$k $v $date")
      }
    }
  }

  def pullProbe(start:Date, stop:Date, metric:String):List[(Date,Server,Key,Value)] = {
    val req = Http(url + "/render")
        .params(
          "from" -> dateFormat.format(start),
          "until" -> dateFormat.format(stop),
          "target" -> s"10sec.*.$metric.*",
          "format" -> "json")
    logger.debug("query: " + req)
    val resp = parse(StringInput(req.asString.body))
    resp.asInstanceOf[JArray].values.flatMap {
      case obj:Map[String,_] =>
        val Array(_,server,_,key) = obj("target").toString.split('.')
        obj("datapoints").asInstanceOf[List[List[_]]].flatMap {
          case v :: ts :: Nil if(v != null) => Some((new Date(1000L*ts.asInstanceOf[BigInt].toLong), server, key, v.asInstanceOf[Double]))
          case _ => None
        }
    }
  }

  import Environment.docker
  import com.github.dockerjava.api.model._
  def doStartContainer {
    val image = "timebench-graphite"
    val is = docker.buildImageCmd(new java.io.File("docker/graphite")).withTag(image).exec()
    Iterator.continually (is.read).takeWhile(-1 !=).foreach( a => () )
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(2003), new ExposedPort(80))
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("2003:2003"), PortBinding.parse("80:80")).exec()
    Retry(103, 10 seconds) { () =>
      new Socket(InetAddress.getByName(Environment.dockerHost), 2003).close()
    }
    pool = new org.apache.commons.pool2.impl.GenericObjectPool(factory)
  }

  override def stopContainer = {
    if(pool != null) {
      pool.close
      pool = null
    }
    super.stopContainer
  }

  def diskDataPath = "/opt/graphite/storage/whisper"

}

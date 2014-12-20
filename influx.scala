package bench

import Types._

import java.util.Date
import scala.concurrent.duration._

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.ExposedPort

import scalaj.http._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object InfluxDBStore extends StoreInterface {
  def hostname = Environment.dockerHost
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
  dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  val request = Http(s"http://$hostname:8086/db/test/series?u=test&p=test&time_precision=s")
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val date = timestamp.getTime() / 1000
    val body = ( "name" -> "test") ~
      ("columns" -> List("time", "server", "probe", "key", "value") ) ~
      ("points" -> values.map { case (s,p,k,v) => JArray(List(date, s, p, k ,v)) } )
    val code = request.postData(compact(render(JArray(List(body))))).asBytes.code
  }

  def pullProbe(start:Date, stop:Date, probe:Probe):List[(Date,Server,Key,Value)] = {
    val req = request
        .param("q", "select time,server,key,value from test where time > '%s' and time < '%s' and probe = '%s'"
                    .format(dateFormat.format(start),
                            dateFormat.format(stop), probe))
      .method("GET")
    logger.debug { "start:" + start + " -> " + dateFormat.format(start) }
    logger.debug { "stop :" + stop  + " -> " + dateFormat.format(stop) }
    logger.debug { s"query:$req" }
    val resp = parse(StringInput(req.asString.body))
    if(resp.values.asInstanceOf[List[_]].size == 0)
      return List()
    logger.debug { " --- > " + resp.values.asInstanceOf[List[_]].size }
    val columns = (resp(0) \ "columns").values.asInstanceOf[List[_]]
    val iTime = columns.indexOf("time")
    val iServer = columns.indexOf("server")
    val iKey = columns.indexOf("key")
    val iValue = columns.indexOf("value")
    (resp(0) \ "points").values.asInstanceOf[List[List[_]]].map { a =>
      ( new Date(a(iTime).asInstanceOf[BigInt].toLong * 1000),
        a(iServer).toString, a(iKey).toString, a(iValue).asInstanceOf[Double])
    }
  }

  import Environment.docker
  import com.github.dockerjava.api.model._
  def doStartContainer {
    val image = "tutum/influxdb"
    docker.pullImageCmd(image).exec()
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(8090), new ExposedPort(8099))
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("8083:8083"), PortBinding.parse("8086:8086")).exec()
    Retry(30, 1 second) { () =>
      Http(s"http://$hostname:8086/db?u=root&p=root").postData(compact(render(("name"->"test")))).asString
    }
    Http(s"http://$hostname:8086/db/test/users?u=root&p=root").postData(compact(render(("name"->"test") ~ ("password"->"test")))).asString
  }

  def diskDataPath = "/data"
}

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

  val request = Http(s"http://$hostname:8086/db/test/series?u=test&p=test&time_precision=s")
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val date = timestamp.getTime() / 1000
    val body = ( "name" -> "test") ~
      ("columns" -> List("time", "server", "probe", "key", "value") ) ~
      ("points" -> values.map { case (s,p,k,v) => JArray(List(date, s, p, k ,v)) } )
    val code = request.postData(compact(render(JArray(List(body))))).asBytes.code
  }

  def pullProbe(start:Date, stop:Date, metric:String):List[(Date,Server,Key,Value)] = List()

  import Environment.docker
  import com.github.dockerjava.api.model._
  def doStartContainer {
    val image = "tutum/influxdb:latest"
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

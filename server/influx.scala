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

case class InfluxDBStore(hostname:String, portHttp:Int, db:String, user:String, pwd:String)
    extends StoreInterface {
  val request = Http(s"http://$hostname:$portHttp/db/$db/series?u=$user&p=$pwd&time_precision=s")
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val date = timestamp.getTime() / 1000
    val body = ( "name" -> "test") ~
      ("columns" -> List("time", "server", "probe", "key", "value") ) ~
      ("points" -> values.map { case (s,p,k,v) => JArray(List(date, s, p, k ,v)) } )
    val code = request.postData(compact(render(JArray(List(body))))).asBytes.code
  }

  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()

  import Environment.docker
  def startContainer {
    val image = "tutum/influxdb:latest"
    docker.pullImageCmd(image).exec()
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(8090), new ExposedPort(8099))
      .withPortSpecs("8083:8083","8086:8086")
      .withEnv("PRE_CREATE_DB=test")
      .exec()
    docker.startContainerCmd(containerName).exec()
  }
}

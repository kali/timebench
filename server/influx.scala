package bench

import Types._

import java.util.Date
import scala.concurrent.duration._

case class InfluxDBStore(hostname:String, portHttp:Int, db:String, user:String, pwd:String)
    extends StoreInterface {
  import scalaj.http._
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  val request = Http(s"http://$hostname:$portHttp/db/$db/series?u=$user&p=$pwd&time_precision=s")
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val date = timestamp.getTime() / 1000
    val body = ( "name" -> "test") ~
      ("columns" -> List("time", "server", "probe", "key", "value") ) ~
      ("points" -> values.map { case (s,p,k,v) => JArray(List(date, s, p, k ,v)) } )
    val code = request.postData(compact(render(JArray(List(body))))).asBytes.code
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
}

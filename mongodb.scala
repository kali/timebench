package bench

import Types._

import java.util.Date
import scala.concurrent.duration._

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.ExposedPort

import com.mongodb.casbah.Imports._

import scalaj.http._

abstract class MongoDBContainer extends StoreInterface {
  import Environment.docker
  import com.github.dockerjava.api.model._
  def hostname = Environment.dockerHost
  def doStartContainer {
    val image = "dockerfile/mongodb"
    val is = docker.pullImageCmd(image).exec()
    drain(is)
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(27017), new ExposedPort(28017))
      .withCmd("mongod", "--httpinterface", "--rest")
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("27017:27017"),PortBinding.parse("28017:28017")).exec()
    Retry(60, 10 second) { () => Http(s"http://$hostname:28017/").asString }
  }

  def diskDataPath = "/data"
  var client = MongoConnection(Environment.dockerHost)
  override def stopContainer {
    client.close
    client = MongoConnection(Environment.dockerHost)
    super.stopContainer
  }
}

trait MongoDBFlatStorage {
  def logger:org.slf4j.Logger
  def collection:MongoCollection
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    collection.insert(values.map {
      case (s,p,k,v) => MongoDBObject("s" -> s, "t" -> timestamp, "p" -> p, "k" -> k, "v" -> v)
    }:_*)
  }
  def pullProbe(start:Date, stop:Date, probe:Probe):List[(Date,Server,Key,Value)] = {
    val query = MongoDBObject(  "t" -> MongoDBObject("$lt" -> stop, "$gte" -> start),
                                    "p" -> probe)
    logger.debug("query: " + query)
    collection.find(query, MongoDBObject("t" -> 1, "s" -> 1, "k" -> 1, "v" -> 1))
      .map{ doc => (doc.getAs[Date]("t").get,doc.getAs[Server]("s").get,
                    doc.getAs[Key]("k").get,doc.getAs[Double]("v").get) }.toList
  }
}

case object NaiveMongoDBStore extends MongoDBContainer with MongoDBFlatStorage {
  def collection = client("test")("naive")
}

case object IndexedMongoDBStore extends MongoDBContainer with MongoDBFlatStorage {
  def collection = client("test")("indexed")
  override def doStartContainer {
    super.doStartContainer
    collection.ensureIndex(MongoDBObject("p" -> 1, "ts" -> 1, "s" -> 1, "v" -> 1))
  }
}

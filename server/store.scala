package bench

import java.util.Date
import akka.actor._

import scala.concurrent._
import ExecutionContext.Implicits.global

import com.codahale.metrics._
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._
import com.github.dockerjava.api.DockerClient

object Types {
  type Server = String
  type Probe = String
  type Key = String
  type Value = Double
}
import Types._

trait StoreInterface {
  def startContainer(implicit docker:DockerClient)
  def stopContainer(implicit docker:DockerClient)

  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)])
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:Probe):Iterator[(Date,Server,Key,Value)]

}

object NotAStore extends StoreInterface {
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
  }
  def pullProbe(start:Date, stop:Date, interval:Duration, metric:String):Iterator[(Date,Server,Key,Value)] = Iterator()
  def startContainer(implicit docker:DockerClient) {}
  def stopContainer(implicit docker:DockerClient) {}
}



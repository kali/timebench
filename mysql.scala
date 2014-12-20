package bench

import Types._

import java.util.Date
import scala.concurrent.duration._

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.ExposedPort

import com.mongodb.casbah.Imports._

import javax.sql.DataSource
import java.sql.Connection
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.dbcp2._

object MysqlStore extends StoreInterface {
  lazy val dataSource:DataSource = {
    import Environment.dockerHost
    val connectionFactory =
      new DriverManagerConnectionFactory(s"jdbc:mysql://$dockerHost/bench","bench","bench")
    val poolableConnectionFactory =
      new PoolableConnectionFactory(connectionFactory, null)

    val connectionPool = new GenericObjectPool(poolableConnectionFactory)
    poolableConnectionFactory.setPool(connectionPool)
    new PoolingDataSource(connectionPool)
  }
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
  dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

  def doStartContainer {
    import javax.management.ObjectName
    import Environment.{ docker, dockerHost }
    import com.github.dockerjava.api.model._
    val image = "library/mysql:5.5"
    docker.pullImageCmd(image).exec()
    docker.createContainerCmd(image).withName(containerName)
      .withExposedPorts(new ExposedPort(3306))
      .withEnv( "MYSQL_ROOT_PASSWORD=dapass","MYSQL_USER=bench",
                "MYSQL_PASSWORD=bench","MYSQL_DATABASE=bench")
      .exec()
    docker.startContainerCmd(containerName).withNetworkMode("bridge")
      .withPortBindings(PortBinding.parse("3306:3306")).exec()

    Retry(30, 1 second) { () =>
      withConnection { c =>
        c.prepareStatement("""CREATE TABLE bench (
          ts timestamp,
          server char(16),
          probe char(8),
          name char(8),
          value double
        )""" ).execute
      }
    }
    withConnection { c =>
      c.prepareStatement("""CREATE INDEX ix_bench ON bench(ts,probe)""" ).execute
    }
}

  def withConnection[T]( f: Connection => T):T = {
    val connection = dataSource.getConnection
    try {
      f(connection)
    } finally {
      connection.close
    }
  }
  def diskDataPath = "/var/lib/mysql"
  def storeValues(timestamp:Date, values:Seq[(Server,Probe,Key,Value)]) {
    val ts = dateFormat.format(timestamp)
    withConnection { c =>
      c.prepareStatement("""INSERT INTO bench VALUES """ +
        values.map { case (s,p,k,v) =>
          s"('$ts', '$s', '$p', '$k', $v)"
        }.mkString(",")).execute
    }
  }
  def pullProbe(start:Date, stop:Date, probe:Probe):List[(Date,Server,Key,Value)] =
    withConnection { c =>
      val startDate = dateFormat.format(start)
      val stopDate = dateFormat.format(stop)
      val rs = c.createStatement.executeQuery(s"""
        SELECT ts, server, name, value FROM bench 
        WHERE probe = '$probe' AND ts >= '$startDate' AND ts < '$stopDate'
        """)
      val buffer = scala.collection.mutable.Buffer[(Date,Server,Key,Value)]()
      while(rs.next) {
        val tuple = (rs.getDate("ts"), rs.getString("server"), rs.getString("name"),
          rs.getDouble("value"))
        buffer.append(tuple)
      }
      buffer.toList
    }
}

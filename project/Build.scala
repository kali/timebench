import sbt._
import sbt.Keys._

object MyApp extends Build {
  val revolver = spray.revolver.RevolverPlugin.Revolver.settings
  lazy val root =
    Project("root", file("."), settings=(List(
      scalaVersion := "2.11.4",
      initialCommands in console := """import bench._; import java.util.Date;""",
      managedResources in Compile += file("logback.xml"),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % "2.3.7",
        "org.scalaj" %% "scalaj-http" % "1.1.0",
        "org.json4s" %% "json4s-jackson" % "3.2.11",
        "org.slf4j" % "slf4j-api" % "1.7.7",
        "org.slf4j" % "jul-to-slf4j" % "1.7.7",
        "ch.qos.logback" % "logback-core" % "1.1.2",
        "ch.qos.logback" % "logback-classic" % "1.1.2",
        "com.github.docker-java" % "docker-java" % "0.10.5",
        "org.mongodb" % "casbah-core_2.11" % "2.7.4",
        "mysql" % "mysql-connector-java" % "5.1.34",
        "org.apache.commons" % "commons-dbcp2" % "2.0.1",
        "org.apache.commons" % "commons-pool2" % "2.2",
        "io.dropwizard.metrics" % "metrics-core" % "3.1.0"
      )
    ) ++ revolver)
  )
}

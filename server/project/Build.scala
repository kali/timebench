import sbt._
import sbt.Keys._

object MyApp extends Build {
  val revolver = spray.revolver.RevolverPlugin.Revolver.settings
  lazy val root =
    Project("root", file("."), settings=(List(
      scalaVersion := "2.11.4",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % "2.3.7",
        "org.scalaj" %% "scalaj-http" % "1.1.0",
        "org.json4s" %% "json4s-jackson" % "3.2.11",
        "com.github.docker-java" % "docker-java" % "0.10.4",
        "io.dropwizard.metrics" % "metrics-core" % "3.1.0"
      )
    ) ++ revolver)
  )
}

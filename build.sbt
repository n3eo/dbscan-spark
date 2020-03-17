organization := "com.esri"

name := "dbscan-spark"

version := "0.8"

scalaVersion := "2.11"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

sparkVersion := "2.4.4"

sparkComponents := Seq("core")

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in assembly := Some("com.esri.dbscan.DBSCANApp")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)
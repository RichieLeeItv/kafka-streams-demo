ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "Kafka streams demo"
  )

libraryDependencies ++= Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.0",
  "com.sksamuel.avro4s" %% "avro4s-kafka" % "4.1.0",
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  "org.apache.kafka" % "kafka-streams" % "3.3.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.3.1"
)

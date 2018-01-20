name := "eventhubs-reingest"

organization := "gs.nathan"

version := "0.1.2-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "com.microsoft.azure" % "azure-eventhubs" % "0.15.1",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

mainClass in assembly := Some("gs.nathan.eventhubsreingest.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
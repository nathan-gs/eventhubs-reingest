name := "eventhubs-reingest"
organization := "gs.nathan"
version := "1.0.0"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "com.microsoft.azure" % "azure-eventhubs" % "0.15.1",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.jmockit" % "jmockit" % "1.34" % "test"
)

mainClass in assembly := Some("gs.nathan.eventhubsreingest.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
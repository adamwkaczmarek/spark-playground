scalaVersion := "2.12.13"
name := "spark-playground"

val sparkVersion = "3.2.0"
val postgresVersion = "42.2.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
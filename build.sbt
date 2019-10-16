name := "DataEngineeringCodingRound"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val catsVersion = "1.6.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11")
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided exclude("joda-time", "joda-time")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.5" % Test



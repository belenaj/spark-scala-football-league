name := "spark-scala-football-league"

version := "0.1"

scalaVersion := "2.12.12"

val SparkVersion = "2.4.3"
val SparkTestingVersion = s"${SparkVersion}_0.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"  % SparkVersion % Provided,
  "com.holdenkarau" %% "spark-testing-base" % SparkTestingVersion % Test
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

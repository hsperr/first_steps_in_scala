name := "MachineLearning"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0"
)

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"
name := "Rules_Engine"

organization := "com.databricks"

version := "0.1"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.databricks",
  scalaVersion := "2.11.12"
)

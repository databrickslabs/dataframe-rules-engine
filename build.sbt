name := "dataframe-rules-engine"

organization := "com.databricks"

version := "0.1"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

lazy val excludes = jacocoExcludes in Test := Seq()

lazy val jacoco = jacocoReportSettings in test  :=JacocoReportSettings(
  "Jacoco Scala Example Coverage Report",
  None,
  JacocoThresholds (branch = 100),
  Seq(JacocoReportFormats.ScalaHTML,
    JacocoReportFormats.CSV),
  "utf-8")

val jacocoSettings = Seq(jacoco)
lazy val jse = (project in file (".")).settings(jacocoSettings: _*)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")


lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.databricks",
  scalaVersion := "2.11.12"
)

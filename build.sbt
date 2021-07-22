name := "dataframe-rules-engine"

organization := "com.databricks.labs"

version := "0.2.0"

scalaVersion := "2.12.12"
scalacOptions ++= Seq("-Xmax-classfile-name", "78")

// groupId, SCM, license information
homepage := Some(url("https://github.com/databrickslabs/dataframe-rules-engine"))
scmInfo := Some(ScmInfo(url("https://github.com/databrickslabs/dataframe-rules-engine"), "git@github.com:databrickslabs/dataframe-rules-engine.git"))
developers := List(Developer("geeksheikh", "Daniel Tomes", "daniel@databricks.com", url("https://github.com/GeekSheikh")))
licenses += ("Databricks", url("https://github.com/databrickslabs/dataframe-rules-engine/blob/master/LICENSE"))
publishMavenStyle := true

publishTo := Some(
  if (version.value.endsWith("SNAPSHOT"))
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.6" % Test

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

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
  version := "0.2.0",
  organization := "com.databricks.labs",
  scalaVersion := "2.12.12"
)

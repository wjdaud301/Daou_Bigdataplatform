lazy val commonSettings = Seq(
  name := "halfsms",
  version := "0.0.1",
  scalaVersion := "2.12.15"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*)

mainClass in assembly := Some("com.daou.halfsms.Processor")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.10.5",
  "org.joda" % "joda-convert" % "1.8.1",
  "org.apache.spark" %% "spark-streaming" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1"
)





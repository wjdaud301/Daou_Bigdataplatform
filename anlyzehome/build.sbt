val sparkVersion = "3.2.1"

lazy val commonSettings = Seq(
  name := "AnalyzeHome",
  version :=  "0.0.1",
  scalaVersion := "2.12.15"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*)


mainClass in assembly := Some("com.daou.analyzehome.Analyzer")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "io.circe" %% "circe-parser" % "0.14.0",
  "joda-time" % "joda-time" % "2.10.5",
  "org.joda" % "joda-convert" % "1.8.1",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.1.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1"

)



name := "dv-spark-bench"
version := "1.0"
organization := "dv.sparkbench"
scalaVersion := "2.10.4"

// Note the use of the %% vs % sign.
// %% appends scala version while % does not
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0" % "provided"

//resolvers ++= Seq(
//  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
//  "Spray Repository" at "http://repo.spray.cc/",
//  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
//  "Akka Repository" at "http://repo.akka.io/releases/",
//  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
//  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
//  "Twitter Maven Repo" at "http://maven.twttr.com/",
//  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
//  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
//  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
//  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
//  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
//  Resolver.sonatypeRepo("public")
//)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "hadoop", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", "spark", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)         => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*)         => MergeStrategy.first
  case PathList("com", "google", xs @ _*)         => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


  //case PathList("scala", xs @ _*) => MergeStrategy.discard

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

   //libraryDependencies ++= Seq(
    //  "org.apache.spark" %% "spark-core" % "0.8.0-incubating" % "provided",
    //  "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.4.0" % "provided"
    //)


 //"org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
 // "org.apache.spark" %% "spark-sql" % "1.3.1",
 // "org.apache.spark" %% "spark-hive" % "1.3.1",
 // "org.apache.spark" %% "spark-streaming" % "1.3.1",
 // "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1",
 // "org.apache.spark" %% "spark-streaming-flume" % "1.3.1",
 // "org.apache.spark" %% "spark-mllib" % "1.3.1",


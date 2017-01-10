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

// This is for times where dependencies are called multiple times
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "hadoop", xs @ _*)        => MergeStrategy.first
  case PathList("org", "apache", "spark", xs @ _*)         => MergeStrategy.first
  case PathList("com", "google", xs @ _*)                  => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)                  => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*)                   => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*)        => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"       => MergeStrategy.first
  case "application.conf"                                  => MergeStrategy.concat
  case "unwanted.txt"                                      => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//Important line below.  This strips out all the scala dependencies and shrinks down your jar into skinny jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

  
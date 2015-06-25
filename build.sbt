name := "wikidata-akka-streams"

scalaVersion := "2.11.6"

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC4"

scalacOptions in Test ++= Seq("-Yrangepos")

mainClass in Compile := Some("com.intenthq.wikidata.App")
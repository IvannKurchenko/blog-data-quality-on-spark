name := "deequ-evaluation"

version := "0.1.0"

scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.mariadb.jdbc" % "mariadb-java-client" % "3.5.6",
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "com.amazon.deequ" % "deequ" % "2.0.9-spark-3.5"
)

fork := true

javaOptions ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)
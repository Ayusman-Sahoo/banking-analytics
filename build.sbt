// Spark 3.5.5 artifacts use the Scala 2.13 binary interface.
scalaVersion := "2.13.16"

lazy val root = rootProject
  .settings(
    name := "banking-analytics",
    Compile / run / fork := true,
    Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.13" % "3.5.5",
      "org.apache.spark" % "spark-sql_2.13" % "3.5.5",
      //You can add library dependencies here, for example,
      //"org.scalatest" %% "scalatest" % "3.2.19" % Test,
      //"org.scalameta" %% "munit" % "1.2.3" % Test
    )
  )

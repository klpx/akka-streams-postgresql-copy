name := "akka-stream-postgresql-copy"

version := "1.0"

scalaVersion := "2.12.1"
crossScalaVersions := Seq("2.12.1", "2.11.8")


libraryDependencies ++= {
  val akkaVer = "2.4.16"

  Seq(
     "com.typesafe.akka" %% "akka-stream"         % akkaVer    % "provided,test"
    ,"com.typesafe.akka" %% "akka-stream-testkit" % akkaVer    % "provided,test"
    ,"org.postgresql"    %  "postgresql"          % "9.4.1212" % "provided,test"

    ,"org.scalatest"          %% "scalatest"   % "3.0.1" % "test"
    ,"com.github.docker-java" %  "docker-java" % "3.0.6" % "test"
  )
}



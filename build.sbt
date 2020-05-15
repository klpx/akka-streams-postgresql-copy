name := "akka-streams-postgresql-copy"
organization := "ru.arigativa"


version := "0.9.0"

scalaVersion := "2.13.1"
crossScalaVersions := Seq("2.13.1", "2.12.11", "2.11.12")

libraryDependencies ++= {
  def akkaVer = scalaVersion.value match {
    case "2.13.1" => "2.5.31"
    case _ => "2.5.12"
  }

  Seq(
     "com.typesafe.akka" %% "akka-stream"         % akkaVer    % "provided,test"
    ,"com.typesafe.akka" %% "akka-stream-testkit" % akkaVer    % "provided,test"
    ,"org.postgresql"    %  "postgresql"          % "9.4.1212" % "provided,test"

    // testing
    ,"org.scalatest" %% "scalatest"    % "3.0.8" % "test"
    ,"org.mockito"   %  "mockito-core" % "2.6.5" % "test"
    ,"com.github.docker-java" % "docker-java" % "3.2.1" % "test"
    ,"org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6" % "test"
  )
}



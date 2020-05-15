logLevel := Level.Warn

// CI
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")

// Publishing
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.2")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.1")

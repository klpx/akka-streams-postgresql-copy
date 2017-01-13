package it

import util.docker.PostgresFixture
import org.scalatest.{FlatSpec, Matchers}

/**
  * Check for integration with Postgres in Docker is working
  */
class HealthcheckSpec extends FlatSpec with Matchers with PostgresFixture {

  "withPostgresConnection" should "provide working connection" in {
    withPostgresConnection(Postgres_9_4_10) { getConn =>
      val conn = getConn()
      val rs = conn.execSQLQuery("SELECT 1+1")
      rs.next()
      rs.getInt(1) shouldBe 2
    }
  }
}

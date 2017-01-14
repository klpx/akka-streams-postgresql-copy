package it

import org.scalatest.{AsyncFlatSpec, Matchers}
import util.PostgresFixture

/**
  * Check for integration with Postgres in Docker is working
  */
class HealthcheckSpec extends AsyncFlatSpec with Matchers with PostgresFixture {

  "withPostgresConnection" should "provide working connection" in {
    withPostgres("healthcheck") { conn =>
      val rs = conn.execSQLQuery("SELECT is_ok FROM status")
      rs.next()
      rs.getBoolean(1) shouldBe true
    }
  }
}

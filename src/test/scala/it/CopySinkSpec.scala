package it

import java.sql.ResultSet

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.github.klpx.akka.PgCopyStreamConverters
import org.scalatest.{AsyncFlatSpec, Matchers}
import util.ActorSystemFixture
import util.docker.PostgresFixture

/**
  * Check for integration with Postgres in Docker is working
  */
class CopySinkSpec extends AsyncFlatSpec with Matchers with PostgresFixture with ActorSystemFixture {

  def fetchPeople(rs: ResultSet): (Long, String, Int) = {
    rs.next()
    (rs.getLong(1), rs.getString(2), rs.getInt(3))
  }

  "PgCopySink" should "copy simple stream with one element" in {
    withActorSystem { implicit system =>
      implicit val mat = ActorMaterializer()
      withPostgresConnection(Postgres_9_4_10) { getConn =>
        val conn1 = getConn()
        conn1.execSQLUpdate("CREATE TABLE people (id SERIAL8, name VARCHAR, age INT)")
        Source.single("Alex\t25\nLisa\t21\n")
          .map(s => ByteString(s))
          .runWith(PgCopyStreamConverters.bytesSink(
            "COPY people (name, age) FROM STDIN",
            conn1
          ))
          .map { rowCount =>
            rowCount shouldBe 2

            val rs = conn1.execSQLQuery("SELECT id, name, age FROM people")
            val firstPeople = fetchPeople(rs)
            val secondPeople = fetchPeople(rs)
            conn1.close()

            firstPeople shouldBe (1L, "Alex", 25)
            secondPeople shouldBe (2L, "Lisa", 21)
          }
      }
    }
  }
}

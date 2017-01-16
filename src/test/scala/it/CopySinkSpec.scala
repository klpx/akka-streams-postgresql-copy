package it

import java.sql.ResultSet

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import ru.arigativa.akka.streams.ConnectionProvider._
import ru.arigativa.akka.streams.PgCopyStreamConverters
import util.PostgresFixture

import scala.util.Random

/**
  * Check for integration with Postgres in Docker is working
  */
class CopySinkSpec extends AsyncFlatSpec with Matchers with PostgresFixture with BeforeAndAfterAll {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  override def afterAll(): Unit = {
    system.terminate()
  }

  def fetchPeople(rs: ResultSet): (Long, String, Int) = {
    rs.next()
    (rs.getLong(1), rs.getString(2), rs.getInt(3))
  }

  "PgCopySink" should "copy simple stream with one element" in {
    withPostgres("people_empty") { conn =>
      Source.single("Alex\t25\nLisa\t21\n")
        .map(s => ByteString(s))
        .runWith(PgCopyStreamConverters.bytesSink(conn, "COPY people (name, age) FROM STDIN"))
        .map { rowCount =>
          rowCount shouldBe 2

          val rs = conn.execSQLQuery("SELECT id, name, age FROM people")
          val firstPeople = fetchPeople(rs)
          val secondPeople = fetchPeople(rs)

          firstPeople shouldBe (1L, "Alex", 25)
          secondPeople shouldBe (2L, "Lisa", 21)
        }
    }
  }

  it should "copy simple Seq of tuples" in {
    val actualFirstPeople = (1L, "Alex", 25)
    val actualSecondPeople = (2L, "Lisa", 21)
    withPostgres("people_empty") { conn =>
      Source.fromIterator(() => Iterator(actualFirstPeople, actualSecondPeople))
        .runWith(PgCopyStreamConverters.sink(conn, "COPY people (id, name, age) FROM STDIN"))
        .map { rowCount =>
          rowCount shouldBe 2

          val rs = conn.execSQLQuery("SELECT id, name, age FROM people")
          val firstPeople = fetchPeople(rs)
          val secondPeople = fetchPeople(rs)
          conn.close()

          firstPeople shouldBe actualFirstPeople
          secondPeople shouldBe actualSecondPeople
        }
    }
  }

  it should "encode special characters correctly" in {
    val actualFirstPeople = (1L, "Alex\r\n\t Hasselbach\\", 25)
    val actualSecondPeople = (2L, "Lisa", 21)
    withPostgres("people_empty") { conn =>
      Source.fromIterator(() => Iterator(actualFirstPeople, actualSecondPeople))
        .runWith(PgCopyStreamConverters.sink(conn, "COPY people (id, name, age) FROM STDIN"))
        .map { rowCount =>
          rowCount shouldBe 2

          val rs = conn.execSQLQuery("SELECT id, name, age FROM people")
          val firstPeople = fetchPeople(rs)
          val secondPeople = fetchPeople(rs)
          conn.close()

          firstPeople shouldBe actualFirstPeople
          secondPeople shouldBe actualSecondPeople
        }
    }
  }
}

package it

import java.sql.ResultSet

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import ru.arigativa.akka.streams.ConnectionProvider._
import ru.arigativa.akka.streams.{PgCopySourceSettings, PgCopyStreamConverters}
import util.PostgresFixture

/**
  * Check for integration with Postgres in Docker is working
  */
class CopySourceSpec extends AsyncFlatSpec with Matchers with PostgresFixture with BeforeAndAfterAll {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  override def afterAll(): Unit = {
    system.terminate()
  }

  def fetchPeople(rs: ResultSet): (Long, String, Int) = {
    rs.next()
    (rs.getLong(1), rs.getString(2), rs.getInt(3))
  }

  "PgCopySource" should "copy bytes as expected" in {
    withPostgres("people_filled") { conn =>
      PgCopyStreamConverters.bytesSource("COPY (SELECT id, name, age FROM people) TO STDOUT", PgCopySourceSettings(conn))
        .runWith(Sink.fold(ByteString.empty) {
          case (acc, next) => acc ++ next
        })
        .map { result =>
          result.decodeString("UTF-8") shouldBe (
            "1\tAlex\t26\n" +
            "2\tLisa\t22\n" +
            "3\tWith\\r\\n\\t special chars\\\\\t10\n" +
            "4\t\\N\t-1\n"
          )
        }
    }
  }

  "PgCopySource" should "copy lines as expected" in {
    withPostgres("people_filled") { conn =>
      PgCopyStreamConverters.source("COPY (SELECT id, name, age FROM people) TO STDOUT", PgCopySourceSettings(conn))
        .runWith(Sink.seq)
        .map { result =>
          result shouldBe Seq(
            Seq("1", "Alex", "26"),
            Seq("2", "Lisa", "22"),
            Seq("3", "With\r\n\t special chars\\", "10"),
            Seq("4", null, "-1")
          )
        }
    }
  }
}

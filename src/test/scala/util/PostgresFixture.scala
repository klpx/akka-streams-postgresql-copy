package util

import org.postgresql.core.BaseConnection
import org.scalatest.{Assertion, BeforeAndAfterAll, Succeeded, Suite}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Created by hsslbch on 9/9/16.
  */
trait PostgresFixture extends BaseFixture with BeforeAndAfterAll { self: Suite =>
  import ExecutionContext.Implicits.global

  /** Execute a test against all supported versions of Postgres in parallel. */
  def withPostgres(fixtureName: String)
                   (testCode: BaseConnection => Future[Assertion]): Future[Assertion] = {
    Future.traverse(PostgresFixture.PostgresImages) { image =>
      val dbName = "test_" + Random.alphanumeric.take(6).mkString.toLowerCase()
      PostgresContainer.withPostgresContainer(image) { connection =>
        connection.createStatement().execute(s"CREATE DATABASE $dbName;")
        connection.createStatement().execute(Resource.read(s"/fixtures/$fixtureName.sql"))

        withClue(s"postgres image: $image") {
          testCode(connection)
        }
      }
    }.map(reduceAssertions)
  }

  /** Assert that every assertion in a collection is successful. */
  private def reduceAssertions(assertions: Set[Assertion]): Assertion = {
    assert(assertions.forall(_ == Succeeded))
  }
}

object PostgresFixture {

  /** Postgres images that will be used for testing. */
  final val PostgresImages: Set[String] = Set(
    "postgres:12-alpine",
    "postgres:11-alpine",
    "postgres:10-alpine",
    "postgres:9-alpine"
  )

}

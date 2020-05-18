package util

import java.sql.{DriverManager, SQLException}

import org.postgresql.core.BaseConnection

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

private case class PostgresContainer(
  containerId: String,
  port: Int,
  username: String,
  password: Option[String]
) {

  val jdbcUrl: String = s"jdbc:postgresql://localhost:$port/"
}

/** Blocking dockerized Postgres container operations. */
private object PostgresContainer {

  import ExecutionContext.Implicits.global

  final val Port = 5432

  /** Run code with a Postgres container that will terminate after use. */
  def withPostgresContainer[T](
    image: String,
    username: String = "postgres",
    password: String = "postgres"
  )(fn: BaseConnection => Future[T]): Future[T] = {
    Docker.withContainer(image) { docker =>
      docker.createContainerCmd(image)
        .withEnv(s"POSTGRES_PASSWORD=$password", s"POSTGRES_USER=$username")
        .withPortSpecs(Port.toString)
        .withPublishAllPorts(true)
    } { docker => container =>

      val hostPort = Try {
        val inspect = docker.inspectContainerCmd(container.getId).exec()
        val portBindings = inspect.getNetworkSettings.getPorts.getBindings.asScala
        portBindings.values.flatten.head.getHostPortSpec.toInt
      }

      val postgresContainer = hostPort.flatMap { hostPort =>
        waitUntilReady(
          PostgresContainer(
            containerId = container.getId,
            port = hostPort,
            username = username,
            password = Some(password)
          )
        )
      }

      Future.fromTry(postgresContainer.flatMap(getConnection)).flatMap(fn)
    }
  }

  /** Retry getting a connection to a container until a maximum number of attempts is reached. */
  private def waitUntilReady(container: PostgresContainer, attempts: Int = 0): Try[PostgresContainer] = {
    if (attempts > 10) {
      Failure(new RuntimeException("Unable to start Postgres container."))
    } else {
      getConnection(container).map { connection =>
        connection.createStatement()
        connection.close()
        container
      }.recoverWith {
        case _: SQLException =>
          Thread.sleep((attempts + 1) * 200)
          waitUntilReady(container, attempts + 1)
      }
    }
  }

  private def getConnection(container: PostgresContainer): Try[BaseConnection] = Try {
    DriverManager
      .getConnection(container.jdbcUrl, container.username, container.password.orNull)
      .asInstanceOf[BaseConnection]
  }
}

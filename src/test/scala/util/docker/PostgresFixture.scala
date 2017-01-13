package util.docker

import java.io.Closeable
import java.sql.DriverManager
import java.time.{Instant, Duration => JDuration}

import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.model.{ExposedPort, Ports, PullResponseItem}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import org.postgresql.core.BaseConnection
import util.BaseFixture

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by hsslbch on 1/13/17.
  */
trait PostgresFixture extends BaseFixture {

  private lazy val dockerConfig =
    DefaultDockerClientConfig.createDefaultConfigBuilder()
      .withDockerHost("unix:///var/run/docker.sock")
      .build()

  private lazy val dockerClient = DockerClientBuilder.getInstance(dockerConfig).build()

  case class PostgresVersion(version: String) {
    def containerName = s"postgres:$version"
  }

  object Postgres_9_4_10 extends PostgresVersion("9.4.10")

  private val username = "postgres"
  private val password = "postgres"

  def pgContainerConnectionTimeout: JDuration = JDuration.ofSeconds(20)
  def pgContainerConnectionTryInterval: JDuration = JDuration.ofMillis(200)


  def withPostgresConnection[T : ClassTag](pgVer: PostgresVersion)(testCode: (() => BaseConnection) => T): T = {
    pullVersion(pgVer)

    val ports = new Ports()
    val postgresPort = new ExposedPort(5432)
    ports.bind(postgresPort, Ports.Binding.empty())

    val container = dockerClient.createContainerCmd(pgVer.containerName)
      .withEnv(s"POSTGRES_USER=$username", s"POSTGRES_PASSWORD=$password")
      .withPortBindings(ports)
      .withPublishAllPorts(true)
      .exec()

    dockerClient.startContainerCmd(container.getId).exec()

    val url = getConnectionUrl(container.getId, postgresPort)

    var conn: BaseConnection = null
    val connCheckStartTime = Instant.now()
    do {
      Thread.sleep(pgContainerConnectionTryInterval.toMillis)
      try {
        conn = DriverManager.getConnection(url, username, password).asInstanceOf[BaseConnection]
        conn.close()
      } catch {
        case _: Throwable =>
      }
      val triesDuration = JDuration.between(connCheckStartTime, Instant.now())
      if (triesDuration > pgContainerConnectionTimeout) {
        throw new RuntimeException("Connection timeout")
      }
    } while (conn == null)

    withOneArg(() => {
      DriverManager.getConnection(url, username, password).asInstanceOf[BaseConnection]
    })(testCode) {
      Try(dockerClient.stopContainerCmd(container.getId).exec())
    }
  }

  private def getConnectionUrl(containerId: String, exposedPort: ExposedPort) = {
    val portBinding =
      dockerClient.inspectContainerCmd(containerId).exec()
        .getNetworkSettings
        .getPorts
        .getBindings
        .get(exposedPort)
        .head
    s"jdbc:postgresql://${portBinding.getHostIp}:${portBinding.getHostPortSpec}/"
  }

  private val pulledContainers = collection.mutable.Set.empty[PostgresVersion]

  private def pullVersion(pgVer: PostgresVersion): Unit = {
    val p = Promise[PullResponseItem]()
    if (!pulledContainers.contains(pgVer)) {
      dockerClient.pullImageCmd(pgVer.containerName).exec(asyncCmdCallback(p))
    }
    println(Await.result(p.future, 1 minute))
  }

  private def asyncCmdCallback[T >: Null](p: Promise[T]) = {
    var lastResult: T = null
    new ResultCallback[T] {

      def onNext(result: T): Unit = {
        lastResult = result
        println(result)
      }

      def onError(throwable: Throwable): Unit = {
        p.tryFailure(throwable)
      }

      def onComplete(): Unit = {
        if (lastResult == null) {
          p.tryFailure(new RuntimeException("No result"))
        } else {
          p.trySuccess(lastResult)
        }
      }

      def onStart(closeable: Closeable): Unit = {}

      def close(): Unit = {}
    }
  }
}

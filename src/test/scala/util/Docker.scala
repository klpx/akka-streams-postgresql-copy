package util

import java.io.Closeable

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.{CreateContainerCmd, CreateContainerResponse}
import com.github.dockerjava.api.model.PullResponseItem
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}

import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

private object Docker {
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Run code against a docker container. */
  def withContainer[T](image: String)(
    createContainer: DockerClient => CreateContainerCmd
  )(
    accessContainer: DockerClient => CreateContainerResponse => Future[T]
  ): Future[T] = {
    // Set up docker client.
    val config = DefaultDockerClientConfig.createDefaultConfigBuilder.build()
    val docker = DockerClientBuilder.getInstance(config).build()

    // Pull the image if it isn't locally cached.
    // This is a blocking call.
    pullContainer(docker, image)

    // Create and start the container.
    val container = Try(createContainer(docker).exec())
    container.map { c =>
      docker.startContainerCmd(c.getId).exec()
    }

    // Run the container access code.
    val accessResult = Future.fromTry(container).flatMap(accessContainer(docker))

    // On completion, terminate the container and close the docker client.
    // The future will only complete once that happens.
    accessResult.andThen { case _ =>
      container.map(_.getId).flatMap(stopContainer(docker))
      docker.close()
    }
  }

  /** Attempt to stop a container. */
  private def stopContainer(docker: DockerClient)(id: String): Try[Unit] = {
    Try(docker.stopContainerCmd(id).exec())
  }

  /** Pull container if it's not already local. */
  private def pullContainer(docker: DockerClient, image: String): Try[Unit] = Try {
    val images = docker.listImagesCmd().exec().asScala
    if (images.exists(_.getRepoTags.contains(image))) {
      () // Do nothing.
    } else {
      val promise = Promise.apply[Unit]()
      docker.pullImageCmd(image).exec(new ResultCallback[PullResponseItem] {
        override def onStart(closeable: Closeable): Unit = ()
        override def onNext(`object`: PullResponseItem): Unit = ()
        override def onError(throwable: Throwable): Unit = promise.failure(throwable)
        override def onComplete(): Unit = promise.success(())
        override def close(): Unit = ()
      })

      import scala.concurrent.Await
      import scala.concurrent.duration._

      // Block and wait for the image to pull.
      Await.result(promise.future, 2.minutes)
    }
  }

}

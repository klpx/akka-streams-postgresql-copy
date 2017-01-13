package util

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect._
import scala.util.{Failure, Success, Try}

/**
  * Created by hsslbch on 11/11/16.
  */
trait BaseFixture {
  import ExecutionContext.Implicits.global

  def withOneArg[A, T](arg: A)(testCode: A => T)(tearDown: => Unit)
                      (implicit ct: ClassTag[T]): T =
    Try(testCode(arg)) match {
      case Failure(err) =>
        tearDown
        throw err
      case Success(result) =>
        classTag[Future[_]].unapply(result) match {
          case Some(futureResult) =>
            val outcomePromise = Promise[Any]()
            futureResult onComplete { result =>
              (result, Try(tearDown)) match {
                case (_, Success(_)) => outcomePromise.complete(result)
                case (_, Failure(tearDownError)) => outcomePromise.complete(Failure(tearDownError))
              }
            }
            outcomePromise.future.asInstanceOf[T]
          case None =>
            tearDown
            result
        }
    }
}

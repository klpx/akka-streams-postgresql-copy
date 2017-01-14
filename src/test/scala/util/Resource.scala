package util

import scala.io.Source

/**
  * Created by hsslbch on 20.04.16.
  */
object Resource {
  def readLines(path: String): Iterator[String] =
    try {
      Source.fromInputStream(
        getClass.getResourceAsStream(path)
      ).getLines()
    } catch {
      case e: NullPointerException => throw new Error(s"Cant read resource `$path`", e)
      case e: Throwable => throw e
    }

  def read(path: String): String = readLines(path).mkString("\n")
}

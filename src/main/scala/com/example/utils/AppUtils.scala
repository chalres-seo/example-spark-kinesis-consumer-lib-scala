package com.example.utils

import java.io.IOException
import java.nio.file.{Files, Paths}

import com.typesafe.scalalogging.LazyLogging
import com.example.utils.retry.Retry

import scala.util.{Failure, Success, Try}

object AppUtils extends LazyLogging {
  def checkDirAndIfNotExistCreate(filePathString: String): Boolean = {
    logger.debug("check file path and create.")
    val path = Paths.get(filePathString)

    Retry.retry {
      if (Files.notExists(path.getParent)) {
        logger.debug(s"dir is not exist. create: ${path.getParent}")
        Try(Files.createDirectory(path.getParent)) match {
          case Success(result) =>
            logger.debug(s"succeed create dir. dir: $result")
            path.getParent == result
          case Failure(e: IOException) =>
            logger.error(s"failed create dir. path: ${path.getParent}")
            logger.error(e.getMessage)
            false
          case Failure(t: Throwable) =>
            logger.error(s"failed create dir. path: ${path.getParent}")
            logger.error(t.getMessage, t)
            false
        }
      } else {
        logger.debug("file path is exist.")
        true
      }
    }
  }
}

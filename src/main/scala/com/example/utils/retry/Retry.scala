package com.example.utils.retry

import com.typesafe.scalalogging.LazyLogging
import com.example.utils.AppConfig

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait Retry extends LazyLogging {
  private val backoffTimeInMillis: Long = AppConfig.backoffTimeInMillis

  def backoff(millis: Long): Unit = {
    logger.debug(s"back off $millis millis.")
    threadSleep(millis)
  }

  def backoff(): Unit = {
    logger.debug(s"back off $backoffTimeInMillis millis.")
    threadSleep(backoffTimeInMillis)
  }

  def threadSleep(): Unit = {
    this.threadSleep(backoffTimeInMillis)
  }

  def threadSleep(millis: Long): Unit = {
    try {
      Thread.sleep(millis)
    } catch {
      case e: InterruptedException =>
        logger.error("interrupted sleep", e)
      case t: Throwable =>
        logger.error("unknown exception thread sleep")
        logger.error(t.getMessage, t)
    }
  }
}

object Retry extends LazyLogging with Retry {
  private val backoffTimeInMillis: Long = AppConfig.backoffTimeInMillis
  private val attemptMaxCount: Int = AppConfig.attemptMaxCount

  def retry[T](fn: => T): T = this.retryWithBackoff(attemptMaxCount, backoffTimeInMillis)(fn)

  @throws(classOf[Exception])
  @tailrec
  def retryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => T): T = {
    Try(fn) match {
      case Success(result) => result

      case Failure(e: Exception) if attemptCount > 0 =>
        logger.error(s"limit exceeded, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        retryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
        throw t
    }
  }
}
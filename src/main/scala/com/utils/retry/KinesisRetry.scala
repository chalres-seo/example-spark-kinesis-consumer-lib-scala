package com.utils.retry

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.model._
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object KinesisRetry extends LazyLogging with Retry {
  private val backoffTimeInMillis: Long = AppConfig.backoffTimeInMillis
  private val attemptMaxCount: Int = AppConfig.attemptMaxCount

  @throws(classOf[Exception])
  def apiRetry[T](fn: => T): Try[T] = Try(this.apiRetryWithBackoff(attemptMaxCount, backoffTimeInMillis)(fn))

  @throws(classOf[Exception])
  @tailrec
  private def apiRetryWithBackoff[T](attemptCount: Int, backoffMillis: Long)(fn: => T): T = {
    Try(fn) match {
      case Success(result) => result

      case Failure(e: ResourceNotFoundException) =>
        logger.error(s"resource not found exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: ResourceInUseException) =>
        logger.error(s"resource in use exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: LimitExceededException) if attemptCount > 0 =>
        logger.error(s"limit exceeded, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        apiRetryWithBackoff(attemptCount - 1, backoffMillis)(fn)

      case Failure(e: InvalidArgumentException) =>
        logger.error(s"invalid argument exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: ProvisionedThroughputExceededException) =>
        logger.error(s"provisioned throughput exceeded exception. the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(t: Throwable) =>
        if (attemptCount <= 0) logger.debug("attempts has been exceeded.")
        else logger.error(s"unknown exception.")
        logger.error(t.getMessage, t)
        throw t
    }
  }
}
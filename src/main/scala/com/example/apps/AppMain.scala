package com.example.apps

import java.nio.file.StandardOpenOption

import com.example.spark.SparkSessionCreator
import com.example.spark.consumer.SparkKinesisConsumer
import com.example.spark.consumer.handler.SparkKinesisConsumerHandler
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreator.getSparkSession

    logger.debug("start kinesis consumer spark-streaming app.")
    logger.debug(s"spark version: ${spark.version}")


    val sparkConsumerFuture: Future[Unit] = Future {
      SparkKinesisConsumer.consume(rdd => {
        SparkKinesisConsumerHandler.printStdoutWithTake(10, rdd)
        SparkKinesisConsumerHandler.printDebugWithTake(10, rdd)
        SparkKinesisConsumerHandler.tmpFileout(1, "tmp/fileoutdir", rdd)
      })
    }

    logger.debug("watch for spark consumer.")
    Await.result(sparkConsumerFuture, Duration.Inf)
  }
}
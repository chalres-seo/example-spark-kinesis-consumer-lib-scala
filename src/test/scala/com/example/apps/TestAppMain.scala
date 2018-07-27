package com.example.apps

import com.example.spark.Spark
import com.example.spark.consumer.SparkKinesisConsumer
import com.example.spark.consumer.handler.SparkKinesisConsumerHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.junit.Test

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class TestAppMain extends LazyLogging {
  val localSpark = Spark.getSparkSession("local[4]")

  @Test
  def testConsumeAppMain(): Unit = {
    logger.debug("start kinesis consumer spark-streaming app.")
    logger.debug(s"spark version: ${localSpark.version}")


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

  @Test
  def testSpark(): Unit = {
    logger.debug("test spark")
    logger.debug(s"spark version: ${localSpark.version}" )

    val sampleDF: DataFrame = localSpark.read.format("csv").option("header", "true").load("tmp/WomensClothingE-CommerceReviews.csv")

    sampleDF.show(10)
  }

  @Test
  def testReadS3(): Unit = {
    val sampleDF: DataFrame = localSpark.read.option("header", "true").csv("tmp/WomensClothingE-CommerceReviews.csv")

    sampleDF.show(10)
  }
}

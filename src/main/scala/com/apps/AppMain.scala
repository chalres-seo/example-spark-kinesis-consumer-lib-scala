package com.apps

import com.spark.SparkSessionCreator
import com.spark.consumer.SparkKinesisConsumer
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

object AppMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreator.getSparkSession

    logger.debug("start kinesis consumer spark-streaming app.")
    logger.debug(s"spark version: ${spark.version}")

    SparkKinesisConsumer.consume()
  }
}

package com.example.spark.consumer.handler

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global

trait SparkKinesisConsumerHandler {
  type RDDHandler[T] = RDD[T] => Unit
}

object SparkKinesisConsumerHandler extends SparkKinesisConsumerHandler with LazyLogging {
  def printStdoutWithTake[T](takeCount: Int, rdd: RDD[T]): Unit = {
    Future {
      logger.debug(s"start limited stdout start. limit count: $takeCount")
      rdd.take(takeCount).foreach(println)
    }
  }

  def printDebugWithTake[T](takeCount: Int, rdd: RDD[T]): Unit = {
    Future {
      logger.debug(s"start limited print debug start. limit count: $takeCount")
      logger.debug(s"total record count: ${rdd.count()}")
      logger.debug(s"total partition count: ${rdd.partitions.length}")

      rdd.take(takeCount).foreach(s => logger.debug(s.toString))
    }
  }

  def tmpFileout(saveFileCount: Int, dirPathString: String, rdd: RDD[String]): Unit = {
    Future {
      logger.debug(s"start tmp file out handler.")
      logger.debug(s"tmp file out dir: $dirPathString")
      rdd.coalesce(saveFileCount).saveAsTextFile(dirPathString)
    }
  }
}
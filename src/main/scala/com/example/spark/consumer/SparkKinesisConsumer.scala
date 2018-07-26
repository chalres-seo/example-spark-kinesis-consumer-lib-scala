package com.example.spark.consumer

import com.example.aws.kinesis.KinesisSdkClient
import com.example.spark.Spark
import com.example.spark.consumer.handler.SparkKinesisConsumerHandler
import com.typesafe.scalalogging.LazyLogging
import com.example.utils.AppConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.TrimHorizon
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.util.{Failure, Success}

object SparkKinesisConsumer extends LazyLogging {
  private val consumerAppName = AppConfig.consumeAppName

  private val consumeKinesisResion = AppConfig.awsRegion
  private val consumeKinesisStream: String = AppConfig.consumeKinesisStream
  private val consumeKinesisEndpoint = AppConfig.consumeKinesisEndpoint

  private val consumeBatchIntervalSec: Duration = Seconds(AppConfig.consumeBatchIntervalSec)

  private val parallelLevel = AppConfig.sparkPalleleLevel

  private val spark: SparkSession = Spark.getSparkSession

  def consume(handler: SparkKinesisConsumerHandler.RDDHandler[String]): Unit = {
    logger.debug("spark kinesis consumer start.")

    val kinesisSdkClient: KinesisSdkClient = KinesisSdkClient.apply()

    logger.debug("check consume stream and create.")

    if (kinesisSdkClient.isNotStreamExist(consumeKinesisStream)) {
      kinesisSdkClient.createStream(consumeKinesisStream)
      kinesisSdkClient.waitStreamReady(consumeKinesisStream)
    }
    require(kinesisSdkClient.isStreamReady(consumeKinesisStream), s"stream is not ready. name: $consumeKinesisStream")

    val streamShardCount: Int = kinesisSdkClient.getShardList(consumeKinesisStream) match {
      case Success(list) => list.size
      case Failure(_) => 0
    }
    require(streamShardCount > 0, s"invalided stream shard count. count: $streamShardCount")

    logger.debug("create spark stream context.")
    val ssc = new StreamingContext(spark.sparkContext, consumeBatchIntervalSec)
    val recordsDStream: DStream[String] = ssc.union {
      (0 until streamShardCount).map { _ =>
        KinesisInputDStream.builder
          .streamingContext(ssc)
          .streamName(consumeKinesisStream)
          .endpointUrl(consumeKinesisEndpoint)
          .regionName(consumeKinesisResion)
          .initialPosition(new TrimHorizon())
          .checkpointAppName(consumerAppName)
          .checkpointInterval(consumeBatchIntervalSec)
          .build()
      }
    }.repartition(parallelLevel).map(new String(_))

    recordsDStream.print
    recordsDStream.foreachRDD(rdd => {
      logger.debug("consume record loop: ")

      if (rdd.isEmpty()) {
        logger.debug("rdd is empty. go to next loop")
      } else this.processRDD(rdd)(handler)
    })

    logger.debug("start spark stream,")
    ssc.start()
    ssc.awaitTermination()
  }

  def processRDD[T](rdd: RDD[T])(handler: SparkKinesisConsumerHandler.RDDHandler[T]): Unit = {
    // RDD process code.
    handler(rdd)
  }
}
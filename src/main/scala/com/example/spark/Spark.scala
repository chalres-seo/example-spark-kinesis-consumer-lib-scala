package com.example.spark

import com.example.aws.credentials.CredentialsFactory
import com.typesafe.scalalogging.LazyLogging
import com.example.utils.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
object Spark extends LazyLogging {
  private val awsCredentitlas = CredentialsFactory.getCredentialsProvider.getCredentials

  private val sparkConf = new SparkConf()
    .setMaster(AppConfig.sparkMaster)
    .set("spark.hadoop.fs.s3a.access.key", awsCredentitlas.getAWSAccessKeyId)
    .set("spark.hadoop.fs.s3a.secret.key", awsCredentitlas.getAWSSecretKey)
    .setAll(AppConfig.sparkConfig)

  private val appName = AppConfig.consumeAppName

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }
}

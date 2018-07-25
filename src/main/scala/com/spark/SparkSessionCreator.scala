package com.spark

import com.aws.credentials.CredentialsFactory
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
object SparkSessionCreator extends LazyLogging {
  private val awsCredentitlas = CredentialsFactory.getCredentialsProvider.getCredentials

  private val sparkConf = new SparkConf()
    .setMaster(AppConfig.sparkMaster)
    .set("spark.hadoop.fs.s3a.access.key", awsCredentitlas.getAWSAccessKeyId)
    .set("spark.hadoop.fs.s3a.secret.key", awsCredentitlas.getAWSSecretKey)
    .setAll(AppConfig.sparkConfig)

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }
}

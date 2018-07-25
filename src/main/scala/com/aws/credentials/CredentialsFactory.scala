package com.aws.credentials

import java.util.concurrent.ConcurrentHashMap

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.collection.JavaConverters._
import scala.collection.concurrent

object CredentialsFactory extends LazyLogging {
  private val awsProfile: String = AppConfig.awsProfile

  private val credentialsProviderList: concurrent.Map[String, AWSCredentialsProvider] =
    new ConcurrentHashMap[String, AWSCredentialsProvider]().asScala

  def getCredentialsProvider: AWSCredentialsProvider = {
    logger.debug("get aws default credentials provider")

    credentialsProviderList.getOrElseUpdate(awsProfile, this.createCredentialsProvider)
  }

  def getCredentialsProvider(profileName: String): AWSCredentialsProvider = {
    logger.debug(s"get aws profile credentials provider, profile : $profileName")

    credentialsProviderList.getOrElseUpdate(profileName, this.createCredentialsProvider(profileName))
  }

  private def createCredentialsProvider: DefaultAWSCredentialsProviderChain = {
    logger.debug("create aws default credentials provider")

    DefaultAWSCredentialsProviderChain.getInstance()
  }

  private def createCredentialsProvider(profileName: String): ProfileCredentialsProvider = {
    logger.debug(s"create aws profile credentials provider, profile : $profileName")

    new ProfileCredentialsProvider(profileName)
  }

  def refreshAllProvider(): Unit = {
    credentialsProviderList.foreach(_._2.refresh())
  }
}

package com.aws.kinesis

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.aws.credentials.CredentialsFactory
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig
import com.utils.retry.KinesisRetry

import scala.annotation.tailrec
import scala.collection.concurrent
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


/**
  * Management Kinesis stream.
  *
  * @param awsProfileName target aws profile, default profile name is 'default'.
  * @param awsRegionName target aws region, default region name is 'ap-northeast-2'.
  * @param kinesisClient aws sdk kinesis client.
  */
class KinesisSdkClient(awsProfileName: String, awsRegionName: String, kinesisClient: AmazonKinesisAsync) extends LazyLogging {
  private val DEFAULT_SHARD_COUNT: Int = 1
  private val INTERVAL_MILLIS: Long = AppConfig.backoffTimeInMillis
  private val STREAM_EXIST_CONDITION = Vector("CREATING", "DELETING", "ACTIVE", "UPDATING")

  def createStream(streamName: String, shardCount: Int): Boolean = {
    logger.debug(s"create stream. name: $streamName, count: $shardCount")

    KinesisRetry.apiRetry(kinesisClient.createStream(streamName, shardCount)) match {
      case Success(_) => true
      case Failure(_: ResourceInUseException) => true
      case Failure(_: Throwable) =>
        logger.error(s"failed create stream. name: $streamName, count: $shardCount")
        false
    }
  }

  def createStream(streamName: String): Boolean = this.createStream(streamName, DEFAULT_SHARD_COUNT)

  def createStreamAndWaitReady(streamName: String): Boolean = {
    this.createStream(streamName)
    Await.result(this.watchStreamReady(streamName), Duration.Inf)
  }

  def deleteStream(streamName: String): Boolean = {
    logger.debug(s"delete stream. name: $streamName")

    KinesisRetry.apiRetry(kinesisClient.deleteStream(streamName)) match {
      case Success(_) => true
      case Failure(_: ResourceNotFoundException) => true
      case Failure(_: Throwable) =>
        logger.error(s"failed delete stream. name: $streamName")
        false
    }
  }

  /**
    * Get stream description.
    *
    * @param streamName unchecked stream name.
    * @param exclusiveStartShardId exclusive shard id. (optional)
    *
    * @see [[kinesisClient.describeStream]]
    *
    * @throws LimitExceededException exceeded request limit, retry and throw. [[KinesisRetry]].
    * @throws ResourceNotFoundException throw when stream is not exist.
    *
    * @return stream description.
    */
  @throws(classOf[LimitExceededException])
  @throws(classOf[ResourceNotFoundException])
  private def getStreamDesc(streamName: String, exclusiveStartShardId: Option[String]): Try[StreamDescription] = {
    exclusiveStartShardId match {
      case Some(exclusiveShardId) =>
        logger.debug(s"get stream description exclusive start shardId. name : $streamName, shardId : $exclusiveStartShardId")
        KinesisRetry.apiRetry(kinesisClient.describeStream(streamName, exclusiveShardId).getStreamDescription)
      case None =>
        logger.debug(s"get stream description. name : $streamName")
        KinesisRetry.apiRetry(kinesisClient.describeStream(streamName).getStreamDescription)
    }
  }

  private def getStreamDesc(streamName: String): Try[StreamDescription] = this.getStreamDesc(streamName, Option.empty[String])

  /**
    * Get stream status.
    *
    * @param streamName unchecked stream name.
    *
    * @return stream status. [[StreamStatus]]
    *         Exist status : CREATING, DELETING, ACTIVE, UPDATING
    *         Not exist status(custom) : NOT_EXIST, UNKNOWN
    */
  def getStreamStatus(streamName: String): String = {
    logger.debug(s"get stream status. name : $streamName")

    this.getStreamDesc(streamName) match {
      case Success(streamDescription) => streamDescription.getStreamStatus
      case Failure(_: ResourceNotFoundException) => "NOT_EXIST"
      case Failure(_: Throwable) => "UNKNOWN"
    }
  }

  def isStreamExist(streamName: String): Boolean = {
    logger.debug(s"check stream exists. name : $streamName")

    STREAM_EXIST_CONDITION.contains(this.getStreamStatus(streamName))
  }

  def isNotStreamExist(streamName: String): Boolean = !this.isStreamExist(streamName)

  def isStreamReady(streamName: String): Boolean = {
    logger.debug(s"check stream ready. name : $streamName")

    this.getStreamStatus(streamName) == "ACTIVE"
  }

  def isNotStreamReady(streamName: String): Boolean = !this.isStreamReady(streamName)

  def watchStreamReady(streamName: String, INTERVAL_MILLIS: Long): Future[Boolean] = {
    logger.debug(s"watch stream ready. name : $streamName, interval millis: $INTERVAL_MILLIS")

    @tailrec
    def loop(streamStatus: String): Boolean = {
      streamStatus match {
        case "ACTIVE" =>
          logger.debug(s"stream status is $streamStatus. name : $streamName.")
          true
        case s if s == "CREATING" || s == "UPDATING" =>
          logger.debug(s"stream status is $streamStatus, wait for $INTERVAL_MILLIS millis stream is ready. name :$streamName")
          KinesisRetry.backoff(INTERVAL_MILLIS)
          loop(this.getStreamStatus(streamName))
        case s if s == "DELETING" || s == "NOT_EXIST" =>
          logger.error(s"stream status is $streamStatus, can't watch stream ready. name : $streamName")
          false
      }
    }
    Future(loop(this.getStreamStatus(streamName)))
  }

  def watchStreamReady(streamName: String): Future[Boolean] = this.watchStreamReady(streamName, INTERVAL_MILLIS)

  def waitStreamReady(streamName: String, INTERVAL_MILLIS: Long, duration: Duration): Boolean = {
    logger.debug(s"wait stream ready. name: $streamName, interval millis: $INTERVAL_MILLIS, duration: $duration")

    Await.result(this.watchStreamReady(streamName, INTERVAL_MILLIS), duration)
  }

  def waitStreamReady(streamName: String): Boolean = {
    this.waitStreamReady(streamName, INTERVAL_MILLIS, Duration.Inf)
  }

  def watchStreamDelete(streamName: String, INTERVAL_MILLIS: Long): Future[Boolean] = {
    logger.debug(s"watch stream delete. name : $streamName, interval millis: $INTERVAL_MILLIS")

    @tailrec
    def loop(streamStatus: String): Boolean = {
      streamStatus match {
        case "NOT_EXIST" =>
          logger.debug(s"stream status is $streamStatus. name : $streamName.")
          true
        case "DELETING" =>
          logger.debug(s"stream status is $streamStatus, wait for $INTERVAL_MILLIS millis stream is delete. name :$streamName")
          KinesisRetry.backoff(INTERVAL_MILLIS)
          loop(this.getStreamStatus(streamName))
        case s if s == "ACTIVE" || s == "CREATING" || s == "UPDATING" =>
          logger.error(s"stream status is $streamStatus, can't watch stream delete. name : $streamName")
          false
      }
    }
    Future(loop(this.getStreamStatus(streamName)))
  }

  def watchStreamDelete(streamName: String): Future[Boolean] = this.watchStreamDelete(streamName, INTERVAL_MILLIS)

  def waitStreamDelete(streamName: String, INTERVAL_MILLIS: Long, duration: Duration): Boolean = {
    logger.debug(s"wait stream delete. name: $streamName, interval millis: $INTERVAL_MILLIS, duration: $duration")

    Await.result(this.watchStreamDelete(streamName, INTERVAL_MILLIS), duration)
  }

  def waitStreamDelete(streamName: String): Boolean = {
    this.waitStreamDelete(streamName, INTERVAL_MILLIS, Duration.Inf)
  }

  /**
    * Get stream list.
    *
    * @see [[kinesisClient.listStreams]]
    *
    * @throws LimitExceededException exceeded request limit, retry and throw. [[KinesisRetry]].
    *
    * @return stream name list.
    */
  @throws(classOf[LimitExceededException])
  def getStreamList: Try[Vector[String]] = {
    logger.debug("get stream list.")

    @tailrec
    def loop(currentListStreamResult: ListStreamsResult, currentStreamNames: Vector[String]): Vector[String] = {
      (currentListStreamResult.isHasMoreStreams.booleanValue(), currentStreamNames.nonEmpty) match {
        case (true, true) =>
          logger.debug("list stream result has more result.")
          val nextListStreamResult: ListStreamsResult = KinesisRetry.apiRetry(kinesisClient.listStreams(currentStreamNames.last)).get
          loop(nextListStreamResult, currentStreamNames ++ nextListStreamResult.getStreamNames.asScala.toVector)
        case (true, false) =>
          val nextListStreamResult: ListStreamsResult = KinesisRetry.apiRetry(kinesisClient.listStreams).get
          loop(nextListStreamResult, nextListStreamResult.getStreamNames.asScala.toVector)
        case (false, _) => currentStreamNames
      }
    }

    KinesisRetry.apiRetry(kinesisClient.listStreams())
      .map(listStreamResult => loop(listStreamResult, listStreamResult.getStreamNames.asScala.toVector))
  }

  /**
    * Get shard list.
    *
    * @param streamName unchecked stream name.
    *
    * @see [[getStreamDesc]]
    *
    * @throws LimitExceededException exceeded request limit, retry and throw. [[KinesisRetry]].
    * @throws ResourceNotFoundException throw when stream is not exist.
    *
    * @return shard list.
    */
  @throws(classOf[LimitExceededException])
  @throws(classOf[ResourceNotFoundException])
  def getShardList(streamName: String): Try[Vector[Shard]] = {
    logger.debug(s"get shard list. name : $streamName")

    @tailrec
    def loop(currentStreamDescription: StreamDescription, currentShardList: Vector[Shard]): Vector[Shard] = {
      (currentStreamDescription.isHasMoreShards.booleanValue(), currentShardList.nonEmpty) match {
        case (false, _) => currentShardList
        case (true, true) =>
          val nextStreamDescription = this.getStreamDesc(streamName, Option(currentShardList.last.getShardId)).get
          loop(nextStreamDescription, currentShardList ++ nextStreamDescription.getShards.asScala.toVector)
        case (true, false) =>
          val nextStreamDescription = this.getStreamDesc(streamName).get
          loop(nextStreamDescription, nextStreamDescription.getShards.asScala.toVector)
      }
    }

    this.getStreamDesc(streamName)
      .map(streamDescription => loop(streamDescription, streamDescription.getShards.asScala.toVector))
  }

  /**
    * Get single shard iterator.
    *
    * @param streamName unchecked stream name.
    * @param shardId shardIterator of shardId.
    * @param shardIteratorType shardIterator type. [[ShardIteratorType]].
    *
    * @see [[kinesisClient.getShardIterator]]
    *
    * @throws ResourceNotFoundException stream is not exist.
    * @throws InvalidArgumentException invalid argument error such as shardId or shardIterator.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    *
    * @return single shard iterator.
    */
  @throws(classOf[ResourceNotFoundException])
  @throws(classOf[InvalidArgumentException])
  @throws(classOf[ProvisionedThroughputExceededException])
  def getShardIterator(streamName: String, shardId: String, shardIteratorType: ShardIteratorType): Try[String] = {
    logger.debug(s"get shard iterator. name: $streamName, shardId: $shardId, type: $shardIteratorType")

    KinesisRetry.apiRetry(kinesisClient.getShardIterator(streamName, shardId, shardIteratorType.toString).getShardIterator)
      .recoverWith {
        case e: ResourceInUseException =>
          logger.error(s"failed get shard iterator. stream is not exist, stream: $streamName")
          Failure(e)
        case e: InvalidArgumentException =>
          logger.error(s"failed get shard iterator. invalid argument, stream: $streamName, iterator type: $shardIteratorType")
          Failure(e)
        case e: ProvisionedThroughputExceededException =>
          logger.error(s"failed get shard iterator. provisioned throughput exceeded, stream: $streamName")
          Failure(e)
        case t: Throwable =>
          logger.error(s"failed get shard iterator. unknown exception, stream: $streamName, iterator type: $shardIteratorType")
          Failure(t)
      }
  }

  /**
    * Get shard iterator list.
    *
    * @param streamName unchecked stream name.
    * @param shardIteratorType shardIterator type. [[ShardIteratorType]].
    *
    * @see [[getShardIterator]]
    *
    * @throws ResourceNotFoundException stream is not exist.
    * @throws InvalidArgumentException invalid argument error such as shardId or shardIterator.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    *
    * @return shard iterator list. exclusive shard which failed get shard iterator.
    */

  @throws(classOf[ResourceNotFoundException])
  @throws(classOf[InvalidArgumentException])
  @throws(classOf[ProvisionedThroughputExceededException])
  def getShardIteratorList(streamName: String, shardIteratorType: ShardIteratorType): Try[Vector[String]] = {
    logger.debug(s"get shard iterator list. name: $streamName, type: $shardIteratorType")

    val getShardIteratorFutures: Try[Vector[Future[Try[String]]]] = this.getShardList(streamName).map(shardList => {
      shardList.map(shard => {
        logger.debug(s"get shard iterator parallel. shardId: ${shard.getShardId}")
        Future(this.getShardIterator(streamName, shard.getShardId, shardIteratorType))
      })
    })

    getShardIteratorFutures.map(shardIteratorFutures => {
      for {
        tryShardIterator <- shardIteratorFutures.map(Await.result(_, Duration.Inf))
        if tryShardIterator.isSuccess
      } yield tryShardIterator.get
    })
  }

  /**
    * Put Records with PutRecordsRequest.
    *
    * @param putRecordsRequest provided request.
    *
    * @see [[kinesisClient.putRecords]]
    *
    * @throws ResourceNotFoundException there is no stream to put records.
    * @throws InvalidArgumentException invalid argument error to put records.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    *
    * @return put record result.
    */
  @throws(classOf[Exception])
  def putRecords(putRecordsRequest: PutRecordsRequest): Try[PutRecordsResult] = {
    logger.debug(s"put records request. stream name: ${putRecordsRequest.getStreamName}, count: ${putRecordsRequest.getRecords.size()}")

    KinesisRetry.apiRetry(kinesisClient.putRecords(putRecordsRequest))
      .recoverWith {
        case e:ResourceNotFoundException =>
          logger.error(s"failed put records. stream is not exist, stream: ${putRecordsRequest.getStreamName}")
          Failure(e)
        case e:InvalidArgumentException =>
          logger.error(s"failed put records. invalid argument. stream: ${putRecordsRequest.getStreamName}")
          Failure(e)
        case e:ProvisionedThroughputExceededException =>
          logger.error(s"failed put records. exceeded provisioned throughput, stream: ${putRecordsRequest.getStreamName}")
          Failure(e)
        case t:Throwable =>
          logger.error(s"failed put records. unknown exception, stream: ${putRecordsRequest.getStreamName}")
          Failure(t)
      }
  }

  /**
    * Get Records with GetRecordsRequest.
    *
    * @param getRecordsRequest provided request.
    *
    * @see [[kinesisClient.getRecords]]
    *
    * @throws ResourceNotFoundException there is no stream to get records.
    * @throws InvalidArgumentException invalid argument error to get records.
    * @throws ProvisionedThroughputExceededException available throughput capacity error.
    *
    * @return get record result.
    */
  @throws(classOf[Exception])
  def getRecords(getRecordsRequest: GetRecordsRequest): Try[GetRecordsResult] = {
    logger.debug(s"get records request.")


    KinesisRetry.apiRetry(kinesisClient.getRecords(getRecordsRequest))
      .recoverWith {
        case e:ResourceNotFoundException =>
          logger.error(s"failed get records. stream is not exist.")
          Failure(e)
        case e:InvalidArgumentException =>
          logger.error(s"failed get records. invalid argument.")
          Failure(e)
        case e:ProvisionedThroughputExceededException =>
          logger.error(s"failed get records. exceeded provisioned throughput.")
          Failure(e)
        case t:Throwable =>
          logger.error("failed get records. unknown exception.")
          Failure(t)
      }
  }

  /**
    * Check stream exist and ready.
    *
    * @param streamName unchecked stream name.
    *
    * @return returns true if the stream is ready. Otherwise, return false.
    */
  def checkStreamExistAndReady(streamName: String): Boolean = {
    logger.debug("check stream exist and ready.")

    if (this.isNotStreamExist(streamName)) false
    else if (this.isNotStreamReady(streamName)) this.waitStreamReady(streamName)
    else true
  }
}

object KinesisSdkClient extends LazyLogging {
  private val awsProfile = AppConfig.awsProfile
  private val awsRegion = AppConfig.awsRegion

  private val kinesisAsyncClientList: concurrent.Map[String, AmazonKinesisAsync] =
    new java.util.concurrent.ConcurrentHashMap[String, AmazonKinesisAsync]().asScala

  def apply(profileName: String, regionName: String): KinesisSdkClient = {
    logger.debug(s"construct api client. profileName: $profileName, regionName: $regionName")
    new KinesisSdkClient(profileName,
      regionName,
      this.getKinesisClient(profileName, regionName))
  }

  def apply(profileName: String): KinesisSdkClient = this.apply(profileName, awsRegion)
  def apply(): KinesisSdkClient = this.apply(awsProfile, awsRegion)

  private def getKinesisClient(profileName: String, regionName: String): AmazonKinesisAsync = {
    logger.debug("get kinesis async client")
    logger.debug(s"profile : $profileName, region : $regionName")

    kinesisAsyncClientList
      .getOrElseUpdate(s"$profileName::$regionName", this.createKinesisClient(profileName, regionName))
  }

  private def createKinesisClient(profileName: String, regionName: String): AmazonKinesisAsync = {
    logger.debug("create kinesis async client")
    logger.debug(s"profile : $profileName, region : $regionName")

    AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getCredentialsProvider(profileName))
      .withRegion(regionName)
      .build()
  }
}
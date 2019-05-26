package org.apache.spark.scheduler

import org.apache.spark.broadcast.Broadcast

private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit =
    throw new UnsupportedOperationException
  def isReady(): Boolean = true

  /**
    * Get an application ID associated with the job.
    *
    * @return An application ID
    */
  def applicationId(): String = appId

  /**
    * Get the attempt ID for this run, if the cluster manager supports multiple
    * attempts. Applications run in client mode will not have attempt IDs.
    *
    * @return The application attempt id, if available.
    */
  def applicationAttemptId(): Option[String] = None

  /**
    * Get the URLs for the driver logs. These URLs are used to display the links in the UI
    * Executors tab for the driver.
    * @return Map containing the log names and their respective URLs
    */
  def getDriverLogUrls: Option[Map[String, String]] = None

  def BlockIdMapToMBR(broadcast:Broadcast[_]) ={}

}


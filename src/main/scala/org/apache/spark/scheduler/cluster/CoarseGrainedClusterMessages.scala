package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ExecutorLossReason
import org.apache.spark.util.SerializableBuffer

private[spark] sealed trait mbpCoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  case object RetrieveSparkAppConfig extends mbpCoarseGrainedClusterMessage

  case class SparkAppConfig(
                             sparkProperties: Seq[(String, String)],
                             ioEncryptionKey: Option[Array[Byte]])
    extends mbpCoarseGrainedClusterMessage

  case object RetrieveLastAllocatedExecutorId extends mbpCoarseGrainedClusterMessage

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends mbpCoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String, interruptThread: Boolean)
    extends mbpCoarseGrainedClusterMessage

  sealed trait RegisterExecutorResponse

  case object RegisteredExecutor extends mbpCoarseGrainedClusterMessage with RegisterExecutorResponse

  case class RegisterExecutorFailed(message: String) extends mbpCoarseGrainedClusterMessage
    with RegisterExecutorResponse

  // Executors to driver
  case class RegisterExecutor(
                               executorId: String,
                               executorRef: RpcEndpointRef,
                               hostname: String,
                               cores: Int,
                               logUrls: Map[String, String])
    extends mbpCoarseGrainedClusterMessage

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
                          data: SerializableBuffer) extends mbpCoarseGrainedClusterMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
    : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends mbpCoarseGrainedClusterMessage

  case object StopDriver extends mbpCoarseGrainedClusterMessage

  case object StopExecutor extends mbpCoarseGrainedClusterMessage

  case object StopExecutors extends mbpCoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
    extends mbpCoarseGrainedClusterMessage

  case class SetupDriver(driver: RpcEndpointRef) extends mbpCoarseGrainedClusterMessage

  // Exchanged between the driver and the AM in Yarn client mode
  case class AddWebUIFilter(
                             filterName: String, filterParams: Map[String, String], proxyBase: String)
    extends mbpCoarseGrainedClusterMessage

  // Messages exchanged between the driver and the cluster manager for executor allocation
  // In Yarn mode, these are exchanged between the driver and the AM

  case class RegisterClusterManager(am: RpcEndpointRef) extends mbpCoarseGrainedClusterMessage

  // Request executors by specifying the new total number of executors desired
  // This includes executors already pending or running
  case class RequestExecutors(
                               requestedTotal: Int,
                               localityAwareTasks: Int,
                               hostToLocalTaskCount: Map[String, Int])
    extends mbpCoarseGrainedClusterMessage

  // Check if an executor was force-killed but for a reason unrelated to the running tasks.
  // This could be the case if the executor is preempted, for instance.
  case class GetExecutorLossReason(executorId: String) extends mbpCoarseGrainedClusterMessage

  case class KillExecutors(executorIds: Seq[String]) extends mbpCoarseGrainedClusterMessage

  // Used internally by executors to shut themselves down.
  case object Shutdown extends mbpCoarseGrainedClusterMessage

  //new message
  case class BlockIdToMBR(broadcast:Broadcast[_]) extends mbpCoarseGrainedClusterMessage

}


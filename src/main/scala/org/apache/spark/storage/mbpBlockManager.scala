package org.apache.spark.storage

import org.apache.spark.memory.MemoryManager
import org.apache.spark.network.BlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory.mbpMemoryStore
import org.apache.spark.{MapOutputTracker, SecurityManager, SparkConf}

class mbpBlockManager (executorId: String,
                       rpcEnv: RpcEnv,
                       master: BlockManagerMaster,
                       serializerManager: SerializerManager,
                       conf: SparkConf,
                       memoryManager: MemoryManager,
                       mapOutputTracker: MapOutputTracker,
                       shuffleManager: ShuffleManager,
                       blockTransferService: BlockTransferService,
                       securityManager: SecurityManager,
                       numUsableCores: Int)
  extends BlockManager(executorId ,rpcEnv ,master,serializerManager,conf ,
    memoryManager ,mapOutputTracker ,shuffleManager ,blockTransferService ,securityManager ,numUsableCores ){
  private[spark] override val memoryStore =
    new mbpMemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)

}

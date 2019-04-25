/*
 * Copyright 2017 by mbp Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.storage.memory

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.sql.simba.spatial.MBR
import org.apache.spark.storage.{BlockId, BlockInfoManager, StreamBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
//import com.mbp.spatialPQ

case class Point(x:Double,y:Double){
  override def equals(that: Any): Boolean = {
    that match {
      case p:Point=> (p.x==x)&&(p.y==y)
      case _=>false
    }
  }
}

/*case class MBR(x:Double,y:Double){
  def dist(other:MBR):Double={
    (x-other.x)*(x-other.x)*(y-other.y)*(y-other.y)
  }
}

case class knnSpatialPQ[A,B]()
  extends mutable.LinkedHashMap[A, B] with Logging{
  import java.util.{LinkedList => JLinkedList}

  val neighbours = new mutable.LinkedHashMap[A,JLinkedList[(A,Double)]]

  var distArray: mutable.LinkedHashMap[BlockId,MBR] = _

  override def put(key: A, value:B ): Option[B] = {
    key match {
      case blockrddNew:org.apache.spark.storage.RDDBlockId=>
        val newMBR = distArray.get(blockrddNew)
        if(newMBR.isDefined){
          //TODO:get the mbr of key Block
          val neighbour =  new JLinkedList[(A,Double)]
          //candidate in memory
          neighbours.foreach(x =>{
            x._1 match {
              case rid:org.apache.spark.storage.RDDBlockId =>{
                //TODO:get the mbr of rid Block
                val oldMBR = distArray.get(rid)
                if(oldMBR.isDefined){
                  val dist= newMBR.get.minDist(oldMBR.get)

                  if(neighbour.size()==0){
                    neighbour.add((rid.asInstanceOf[A],dist))
                  }else{
                    var i=0
                    while(i<neighbour.size()&&neighbour.get(i)._2>dist)
                      i = i+1
                    neighbour.add(i,(rid.asInstanceOf[A],dist))
                  }
                  if(neighbour.size()>3)
                    neighbour.pollFirst()
                }
              }
            }
          })
          neighbours+=(key->neighbour)
          val iter = neighbour.iterator()
          while(iter.hasNext){
            moveToTail(iter.next())
          }
        }
    }
    super.put(key,value)
  }

  private def moveToTail(key:(A,Double))={
    super.remove(key._1) match {
      case Some(v) =>super.put(key._1,v)
      //case _=>super.put(key._1,_)
    }
  }

  def getSpatial(key: A):Option[B]={
    key match {
      case blockrdd:org.apache.spark.storage.RDDBlockId=> {
        if(neighbours.get(blockrdd.asInstanceOf[A]).isDefined){
          val iter = neighbours(blockrdd.asInstanceOf[A]).iterator()
          while(iter.hasNext){
            moveToTail(iter.next())
          }
        }
        super.get(key)
      }
      case _=> super.get(key)
    }
  }
  //remove all blockId value in LinkedHashMap or just remove the blockId key
  override def remove(key:A):Option[B]={
    key match{
      case blockrdd:org.apache.spark.storage.RDDBlockId=>{
        neighbours.remove(blockrdd.asInstanceOf[A])
        neighbours.foreach(li=>li._2.remove(key))
        super.remove(blockrdd.asInstanceOf[A])
      }
      case e => super.remove(e)
    }
  }

  override def clear(): Unit ={
    //locs.clear()
    neighbours.clear()
    super.clear()
  }

  def add_dist(): Unit ={
    distArray.put()
  }
}*/
case class knnSpatialPQ2[A,B]()
  extends mutable.LinkedHashMap[A, B] with Logging{
  import java.util.{LinkedList => JLinkedList}
  val neighbours = new mutable.LinkedHashMap[A,JLinkedList[(A,Double)]]
  var distArray: mutable.LinkedHashMap[A,MBR] = new mutable.LinkedHashMap[A,MBR]()
  private def addNew(neighbour:JLinkedList[(A,Double)],x:A,dist:Double)={
    if(neighbour.size()==0){
      neighbour.add((x,dist))
    }else{
      var i=0
      while(i<neighbour.size()&&neighbour.get(i)._2>dist){
        i = i+1
      }
      neighbour.add(i,(x,dist))
    }
    if(neighbour.size()>1){
      neighbour.pollFirst()
    }
  }
  override def put(key: A, value:B ): Option[B] = {

    val newMBR = distArray.get(key)
    if(newMBR.isDefined){
      logInfo("distArray contain this block....hahaha")
      //TODO:get the mbr of key Block
      val neighbour =  new JLinkedList[(A,Double)]
      //candidate in memory
      neighbours.foreach(x =>{

        //TODO:get the mbr of rid Block
        val oldMBR = distArray.get(x._1)
        if(oldMBR.isDefined){
          val dist= newMBR.get.minDist(oldMBR.get)
          addNew(neighbour,x._1,dist)
          if(neighbours.get(x._1).isDefined){
            addNew(neighbours(x._1),key,dist)
          }
        }
      })
      neighbours+=(key->neighbour)
      val iter = neighbour.iterator()
      while(iter.hasNext){
        moveToTail(iter.next())
      }
    }

    super.put(key,value)
  }
  private def moveToTail(key:(A,Double))={
    if(key.isInstanceOf[(A,Double)]){
      super.remove(key._1) match {
        case Some(v) =>super.put(key._1,v)
        //case _=>super.put(key._1,_)
      }
    }

  }
  def getSpatial(key: A):Option[B]={

    if(neighbours.get(key).isDefined){
      logInfo("neighbour contain this block....hahaha")
      val iter = neighbours(key).iterator()
      while(iter.hasNext){
        moveToTail(iter.next())
      }
    }

    super.get(key)

  }
  //remove all blockId value in LinkedHashMap or just remove the blockId key
  override def remove(key:A):Option[B]={
    neighbours.remove(key.asInstanceOf[A])
    neighbours.foreach(li=>li._2.remove(key))
    super.remove(key.asInstanceOf[A])
  }
  override def clear(): Unit ={
    neighbours.clear()
    super.clear()
  }

  def add_dist(block:A,mbr:MBR): Unit ={
    distArray.put(block,mbr)
  }

}

private[spark] class MemoryStore(
                 conf: SparkConf,
                 blockInfoManager: BlockInfoManager,
                 serializerManager: SerializerManager,
                 memoryManager: MemoryManager,
                 blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // TODO: load the thres from index or config
  val distArray = new mutable.LinkedHashMap[BlockId,MBR]()
  //val distArray = SimbaSession.distanceArray
  private val entries= new knnSpatialPQ2[BlockId, MemoryEntry[_]]()

  def add_dist(block:BlockId,mbr:MBR): Unit ={
    entries.add_dist(block,mbr)
  }
  def add_dist(map:List[(BlockId,MBR)]):Unit={
    map.foreach(x=>add_dist(x._1,x._2))
  }

  //private val entries = new knnSpatialPQ(this)
  // TODO: implement this
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }
  private[spark] def evictBlocksToFreeSpace(
                                             blockId: Option[BlockId],
                                             space: Long,
                                             memoryMode: MemoryMode): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.iterator
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair._1
          val entry = pair._2
          if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair._2.size
            }
          }
        }
      }

      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          blockInfoManager.removeBlock(blockId)
        }
      }

      if (freedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
          s"(${Utils.bytesToString(freedMemory)} bytes)")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry.isDefined) {
            dropBlock(blockId, entry.get)
          }
        }
        logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
          s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
        freedMemory
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  /**
    * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
    * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
    *
    * The caller should guarantee that `size` is correct.
    *
    * @return true if the put() succeeded, false otherwise.
    */
  def putBytes[T: ClassTag](
                             blockId: BlockId,
                             size: Long,
                             memoryMode: MemoryMode,
                             _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
    * Attempt to put the given block in memory store as values.
    *
    * It's possible that the iterator is too large to materialize and store in memory. To avoid
    * OOM exceptions, this method will gradually unroll the iterator while periodically checking
    * whether there is enough free memory. If the block is successfully materialized, then the
    * temporary unroll memory used during the materialization is "transferred" to storage memory,
    * so we won't acquire more memory than is actually needed to store the block.
    *
    * @return in case of success, the estimated size of the stored data. In case of failure, return
    *         an iterator containing the values of the block. The returned iterator will be backed
    *         by the combination of the partially-unrolled block and the remaining elements of the
    *         original input iterator. The caller must either fully consume this iterator or call
    *         `close()` on it in order to free the storage memory consumed by the partially-unrolled
    *         block.
    */
  private[storage] def putIteratorAsValues[T](
                                               blockId: BlockId,
                                               values: Iterator[T],
                                               classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[T]()(classTag)

    // Request enough memory to begin unrolling
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, MemoryMode.ON_HEAP)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    while (values.hasNext && keepUnrolling) {
      vector += values.next()
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        // If our vector's size has exceeded the threshold, request more memory
        val currentSize = vector.estimateSize()
        if (currentSize >= memoryThreshold) {
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    if (keepUnrolling) {
      // We successfully unrolled the entirety of this block
      val arrayValues = vector.toArray
      vector = null
      val entry =
        new DeserializedMemoryEntry[T](arrayValues, SizeEstimator.estimate(arrayValues), classTag)
      val size = entry.size
      def transferUnrollToStorage(amount: Long): Unit = {
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, amount)
          val success = memoryManager.acquireStorageMemory(blockId, amount, MemoryMode.ON_HEAP)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }
      // Acquire storage memory if necessary to store this block in memory.
      val enoughStorageMemory = {
        if (unrollMemoryUsedByThisBlock <= size) {
          val acquiredExtra =
            memoryManager.acquireStorageMemory(
              blockId, size - unrollMemoryUsedByThisBlock, MemoryMode.ON_HEAP)
          if (acquiredExtra) {
            transferUnrollToStorage(unrollMemoryUsedByThisBlock)
          }
          acquiredExtra
        } else { // unrollMemoryUsedByThisBlock > size
          // If this task attempt already owns more unroll memory than is necessary to store the
          // block, then release the extra memory that will not be used.
          val excessUnrollMemory = unrollMemoryUsedByThisBlock - size
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, excessUnrollMemory)
          transferUnrollToStorage(size)
          true
        }
      }
      if (enoughStorageMemory) {
        entries.synchronized {
          entries.put(blockId, entry)
        }
        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(
          blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(size)
      } else {
        assert(currentUnrollMemoryForThisTask >= unrollMemoryUsedByThisBlock,
          "released too much unroll memory")
        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = arrayValues.toIterator,
          rest = Iterator.empty))
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(new PartiallyUnrolledIterator(
        this,
        MemoryMode.ON_HEAP,
        unrollMemoryUsedByThisBlock,
        unrolled = vector.iterator,
        rest = values))
    }
  }

  /**
    * Attempt to put the given block in memory store as bytes.
    *
    * It's possible that the iterator is too large to materialize and store in memory. To avoid
    * OOM exceptions, this method will gradually unroll the iterator while periodically checking
    * whether there is enough free memory. If the block is successfully materialized, then the
    * temporary unroll memory used during the materialization is "transferred" to storage memory,
    * so we won't acquire more memory than is actually needed to store the block.
    *
    * @return in case of success, the estimated size of the stored data. In case of failure,
    *         return a handle which allows the caller to either finish the serialization by
    *         spilling to disk or to deserialize the partially-serialized block and reconstruct
    *         the original input iterator. The caller must either fully consume this result
    *         iterator or call `discard()` on it in order to free the storage memory consumed by the
    *         partially-unrolled block.
    */
  private[storage] def putIteratorAsBytes[T](
                                              blockId: BlockId,
                                              values: Iterator[T],
                                              classTag: ClassTag[T],
                                              memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

  require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

  val allocator = memoryMode match {
    case MemoryMode.ON_HEAP => ByteBuffer.allocate _
    case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
  }

  // Whether there is still enough memory for us to continue unrolling this block
  var keepUnrolling = true
  // Number of elements unrolled so far
  var elementsUnrolled = 0L
  // How often to check whether we need to request more memory
  //val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD)
  //  // Memory torequest as a multiple of current bbos size
  //val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR)
  // Initial per-task memory to request for unrolling blocks (bytes).
  val initialMemoryThreshold = unrollMemoryThreshold
  // Keep track of unroll memory used by this particular block / putIterator() operation
  var unrollMemoryUsedByThisBlock = 0L
  // Underlying buffer for unrolling the block
  val redirectableStream = new RedirectableOutputStream
  val chunkSize = if (initialMemoryThreshold > Int.MaxValue) {
    logWarning(s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
      s"is too large to be set as chunk size. Chunk size has been capped to " +
      s"${Utils.bytesToString(Int.MaxValue)}")
    Int.MaxValue
  } else {
    initialMemoryThreshold.toInt
  }
  val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
  redirectableStream.setOutputStream(bbos)
  val serializationStream: SerializationStream = {
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
    ser.serializeStream(serializerManager.wrapStream(blockId, redirectableStream))
  }

  // Request enough memory to begin unrolling
  keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

  if (!keepUnrolling) {
    logWarning(s"Failed to reserve initial memory threshold of " +
      s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
  } else {
    unrollMemoryUsedByThisBlock += initialMemoryThreshold
  }

  def reserveAdditionalMemoryIfNecessary(): Unit = {
    if (bbos.size > unrollMemoryUsedByThisBlock) {
      val amountToRequest = (bbos.size  - unrollMemoryUsedByThisBlock).toLong
      keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
      if (keepUnrolling) {
        unrollMemoryUsedByThisBlock += amountToRequest
      }
    }
  }

  // Unroll this block safely, checking whether we have exceeded our threshold
  while (values.hasNext && keepUnrolling) {
    serializationStream.writeObject(values.next())(classTag)
    elementsUnrolled += 1
    reserveAdditionalMemoryIfNecessary()
  }

  // Make sure that we have enough memory to store the block. By this point, it is possible that
  // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
  // perform one final call to attempt to allocate additional memory if necessary.
  if (keepUnrolling) {
    serializationStream.close()
    if (bbos.size > unrollMemoryUsedByThisBlock) {
      val amountToRequest = bbos.size - unrollMemoryUsedByThisBlock
      keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
      if (keepUnrolling) {
        unrollMemoryUsedByThisBlock += amountToRequest
      }
    }
  }

  if (keepUnrolling) {
    val entry = SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
    // Synchronize so that transfer is atomic
    memoryManager.synchronized {
      releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
      val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
      assert(success, "transferring unroll memory to storage memory failed")
    }
    entries.synchronized {
      entries.put(blockId, entry)
    }
    logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
      blockId, Utils.bytesToString(entry.size),
      Utils.bytesToString(maxMemory - blocksMemoryUsed)))
    Right(entry.size)
  } else {
    // We ran out of space while unrolling the values for this block
    logUnrollFailureMessage(blockId, bbos.size)
    Left(
      new PartiallySerializedBlock(
        this,
        serializerManager,
        blockId,
        serializationStream,
        redirectableStream,
        unrollMemoryUsedByThisBlock,
        memoryMode,
        bbos,
        values,
        classTag))
  }
}

  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.getSpatial(blockId) }
    entry match {
      case None => None
      case Some(ent)=>
        ent match{
          case e: DeserializedMemoryEntry[_] =>
            throw new IllegalArgumentException("should only call getBytes on serialized blocks")
          case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
        }
    }
  }

  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.getSpatial(blockId) }
    entry match {
      case None => None
      case Some(ent)=>
        ent match {
          case e: SerializedMemoryEntry[_] =>
            throw new IllegalArgumentException("should only call getValues on deserialized blocks")
          case DeserializedMemoryEntry(values, _, _) =>
            val x = Some(values)
            x.map(_.iterator)
      }
    }
  }

  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry.isDefined) {
      entry.get match {
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      memoryManager.releaseStorageMemory(entry.get.size, entry.get.memoryMode)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }



  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
    * Amount of storage memory, in bytes, used for caching blocks.
    * This does not include memory used for unrolling.
    */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      val x=entries.get(blockId)
      x.get.size
    }
  }

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.contains(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
    * Reserve memory for unrolling the given block for this task.
    *
    * @return whether the request is granted.
    */
  def reserveUnrollMemoryForThisTask(
                                      blockId: BlockId,
                                      memory: Long,
                                      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
    * Release memory used by this task for unrolling blocks.
    * If the amount is not specified, remove the current task's allocation altogether.
    */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
    * Return the amount of memory currently occupied for unrolling blocks across all tasks.
    */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
    * Return the amount of memory currently occupied for unrolling blocks by this task.
    */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
    * Return the number of tasks currently unrolling blocks.
    */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
    * Log information about current memory usage.
    */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
        s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
        s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
        s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
    * Log a warning for failing to unroll a block.
    *
    * @param blockId ID of the block we are trying to unroll.
    * @param finalVectorSize Final size of the vector before unrolling failed.
    */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
        s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

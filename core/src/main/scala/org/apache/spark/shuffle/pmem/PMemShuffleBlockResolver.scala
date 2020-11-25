/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import com.intel.oap.common.storage.stream.{DataStore,ChunkInputStream}

import java.io._
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileInputStreamManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.shuffle.pmem.PMemManagerInitializer

import com.intel.oap.common.storage.stream.{ChunkInputStream, ChunkOutputStream, DataStore}

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class PMemShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  private val pMemDataStore = PMemManagerInitializer.getDataStore()

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    //ToDo: remove all pMemStream which belongs to the map.
  }

  def getDataOutputStream(shuffleId: Int, mapId: Long, partitionId: Long): ChunkOutputStream = {
      val streamName = shuffleId + "_" + mapId + "_" + partitionId
      val  pMemOutputStream = ChunkOutputStream.getChunkOutputStreamInstance(streamName, pMemDataStore)
      pMemOutputStream
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }
    val resultBuffer = new FileInputStreamManagedBuffer(transportConf)
    val idx = 0;
    for (idx <- startReduceId to endReduceId){
      val name = shuffleId +"_"+ mapId +"_" + idx
      val in = ChunkInputStream.getChunkInputStreamInstance(name,pMemDataStore)
      resultBuffer.addStream(in)
    }
    resultBuffer
  }

  override def stop(): Unit = {}
}


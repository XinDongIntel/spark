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

package org.apache.spark.shuffle.pmem

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.storage.{BlockId, BlockManager}

private[spark] class PMemShuffleReader[K, C](
    handle: PMemShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  // TODO: need metadata store to save shuffleIds
  var shuffleIdList: Array[BlockId] = new Array[BlockId](1)

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val serializerInstance = dep.serializer.newInstance()
    val recordIter = shuffleIdList.foreach(shuffleId =>
      serializerInstance.deserializeStream(new PMemChunkInputStream(shuffleId)).asKeyValueIterator
    )
    recordIter.asInstanceOf[Iterator[Product2[K, C]]]
  }

}

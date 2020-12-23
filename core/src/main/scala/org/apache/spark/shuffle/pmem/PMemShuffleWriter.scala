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

import org.apache.arrow.plasma.PlasmaClient
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.ShuffleBlockId

private[spark] class PMemShuffleWriter[K, V, C](
    shuffleBlockResolver: PMemShuffleBlockResolver,
    handle: PMemShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    serializerManager: SerializerManager)
  extends ShuffleWriter[K, V] with Logging  {

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val PMemChunkOutputStreamArray = (0 until numPartitions).toArray.map(partitionId =>
      new PMemChunkOutputStream(
        ShuffleBlockId(shuffleId, mapId, partitionId),
        serializerManager,
        dep.serializer.newInstance(),
      )
    )

    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val value = record._2
      PMemChunkOutputStreamArray(partitioner.getPartition(key)).write(key, value)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    None
  }

}

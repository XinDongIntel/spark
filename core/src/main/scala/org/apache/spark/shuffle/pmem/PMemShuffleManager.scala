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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.util.collection.OpenHashSet

private[spark] class PMemShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  override val shuffleBlockResolver: PMemShuffleBlockResolver = new PMemShuffleBlockResolver(conf)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new PMemShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long, context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(context.taskAttemptId()) }

    val serializerManager = SparkEnv.get.serializerManager

    handle match {
      case pmemShuffleHandle: PMemShuffleHandle[K @unchecked, V @unchecked, _] =>
        new PMemShuffleWriter(
          shuffleBlockResolver,
          pmemShuffleHandle,
          mapId,
          context,
          serializerManager)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    new PMemShuffleReader(
      handle.asInstanceOf[PMemShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
      }
    }
    true
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }

}

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

import org.apache.spark.{Partitioner, SharedSparkContext, ShuffleDependency, SparkFunSuite}
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.util.Utils
import org.mockito.Mockito.{mock, when}
import org.mockito.MockitoAnnotations

class PMemShuffleWriterReaderSuite extends SparkFunSuite with SharedSparkContext {

  private val shuffleId = 0
  private val numMaps = 5
  private var shuffleHandle: PMemShuffleHandle[Int, Int, Int] = _
  val serializer = new JavaSerializer(conf)
  private val serializerManager = new SerializerManager(serializer, conf)

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    val partitioner = new Partitioner() {
      def numPartitions = numMaps
      def getPartition(key: Any) = Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
    shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.partitioner).thenReturn(partitioner)
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new PMemShuffleHandle(shuffleId, dependency)
    }
  }

  test("PMem Shuffle Writer and Reader dummy test") {
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    val writer = new PMemShuffleWriter[Int, Int, Int](
      null,
      shuffleHandle,
      mapId = 2,
      context,
      serializerManager)
    writer.write(Iterator.empty)

    val reader = new PMemShuffleReader[Int, Int](
      shuffleHandle,
      0,
      1,
      context,
      serializerManager
    )
    intercept[UnsupportedOperationException] {
      reader.read()
    }
  }

}

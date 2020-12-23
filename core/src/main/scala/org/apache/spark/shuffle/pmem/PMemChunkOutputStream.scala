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

import java.io.OutputStream

import org.apache.arrow.plasma.PlasmaClient

import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.BlockId
import org.apache.spark.util.collection.PairsWriter

private[spark] class PMemChunkOutputStream(
    blockId: BlockId,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance)
  extends OutputStream
  with Logging
  with PairsWriter {

  private var os: OutputStream = _
  private var pos: PMemOutputStream = _
  private var objOut: SerializationStream = _
  private var initialized = false
  private var streamOpen = false

  private def initialize(): Unit = {
    pos = new PMemOutputStream()
  }

  def open(): PMemChunkOutputStream = {
    if (!initialized) {
      initialize()
      initialized = true
    }
    os = serializerManager.wrapStream(blockId, pos)
    objOut = serializerInstance.serializeStream(os)
    streamOpen = true
    this
  }

  override def write(b: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  override def write(key: Any, value: Any): Unit = {
    if (!streamOpen) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
  }

  override def close(): Unit = {

  }

  // TODO: To reuse part of oap common chunk api
  class PMemOutputStream extends OutputStream {

    val plasmaClient = new PlasmaClient("", "", 0)

    override def write(b: Int): Unit = {

    }

    override def write(b: Array[Byte]): Unit = {

    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {

    }
  }

}



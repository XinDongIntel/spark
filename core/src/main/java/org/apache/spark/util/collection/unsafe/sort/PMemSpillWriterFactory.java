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

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;

public class PMemSpillWriterFactory {
    public static UnsafeSorterPMemSpillWriter getSpillWriter(
            PMemSpillWriterType writerType,
            UnsafeExternalSorter externalSorter,
            SortedIteratorForSpills sortedIterator,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics) {
        if (writerType == PMemSpillWriterType.WRITE_SORTED_RECORDS_TO_PMEM) {
            return new SortedPMemPageSpillWriter(externalSorter, sortedIterator, writeMetrics, taskMetrics);
        } else if (writerType == PMemSpillWriterType.MEM_COPY_ALL_DATA_PAGES_TO_PMEM_WITHLONGARRAY){
            new PMemWriter(externalSorter, sortedIterator, writeMetrics, taskMetrics);
        } else {
            //Todo: add other types of pmem spill writer here
            return null;
        }
    }
}

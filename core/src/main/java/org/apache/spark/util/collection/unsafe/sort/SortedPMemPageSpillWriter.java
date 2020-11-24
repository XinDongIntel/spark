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

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import java.io.IOException;
import java.util.LinkedHashMap;


public class SortedPMemPageSpillWriter extends UnsafeSorterPMemSpillWriter {
    private MemoryBlock currentPMemPage = null;
    private long currentOffsetInPage = 0L;
    private int currentNumOfRecordsInPage = 0;
    private int currentRecLen = 0;
    private long currentPrefix = 0L;
    //Page -> record number map
    private LinkedHashMap<MemoryBlock,Integer> pageNumOfRecMap = new LinkedHashMap<MemoryBlock,Integer>();
    private int numRecords = 0;

    public SortedPMemPageSpillWriter(
            UnsafeExternalSorter externalSorter,
            SortedIteratorForSpills sortedIterator,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics) {
        super(externalSorter, sortedIterator, writeMetrics, taskMetrics);
    }

    //This write will write all spilled record in physically sorted PMem page.
    @Override
    public void write() throws IOException {
        while (sortedIterator.hasNext()) {
            sortedIterator.loadNext();
            final Object baseObject = sortedIterator.getBaseObject();
            final long baseOffset = sortedIterator.getBaseOffset();
            currentRecLen = sortedIterator.getRecordLength();
            currentPrefix = sortedIterator.getKeyPrefix();
            if (allocatedPMemPages.isEmpty()) {
                MemoryBlock page = allocatePMemPage();
            }
            long pageBaseOffset = currentPMemPage.getBaseOffset();
            long currentOffset = pageBaseOffset + currentOffsetInPage;
            if (currentOffset > pageBaseOffset + currentPMemPage.size()) {
                allocatePMemPage();
            }
            Platform.putInt(
                    null,
                    currentOffset,
                    currentRecLen);
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            currentOffset += uaoSize;
            Platform.putLong(
                    null,
                    currentOffset,
                    currentPrefix);
            currentOffset += Long.BYTES;
            Platform.copyMemory(
                    baseObject,
                    baseOffset,
                    null,
                    currentOffset,
                    currentRecLen);
            currentNumOfRecordsInPage ++;
            pageNumOfRecMap.put(currentPMemPage, currentNumOfRecordsInPage);
            numRecords ++;
        }
    }

    protected MemoryBlock allocatePMemPage(){
        currentPMemPage = super.allocatePMemPage();
        currentOffsetInPage = 0;
        currentNumOfRecordsInPage = 0;
        return currentPMemPage;
    }

    @Override
    public UnsafeSorterIterator getSpillReader() {
        return new SortedPMemPageSpillReader();
    }

    @Override
    public void clearAll() {
        freeAllPMemPages();
    }

    private class SortedPMemPageSpillReader extends UnsafeSorterIterator {
        private MemoryBlock curPage = null;
        private int curPageIdx = -1;
        private int curOffsetInPage = 0;
        private int curNumOfRecInPage = 0;
        private int curNumOfRec = 0;
        private long curRecordAddress = 0;
        private int recordLength;
        private long keyPrefix;

        public SortedPMemPageSpillReader() {
        }
        @Override
        public boolean hasNext() {
            return curNumOfRec < numRecords;
        }

        @Override
        public void loadNext() throws IOException {
            if (curPage == null || curNumOfRecInPage >= pageNumOfRecMap.get(curPage)) {
                moveToNextPMemPage();
            }
            long curPageBaseOffset = curPage.getBaseOffset();
            recordLength = UnsafeAlignedOffset.getSize(null, curPageBaseOffset + curOffsetInPage);
            curOffsetInPage += UnsafeAlignedOffset.getUaoSize();
            keyPrefix = Platform.getLong(null, curPageBaseOffset + curOffsetInPage);
            curOffsetInPage += Long.BYTES;
            curRecordAddress = curPageBaseOffset + curOffsetInPage;
            curNumOfRecInPage ++;
            curNumOfRec ++;
        }

        private void moveToNextPMemPage() {
            curPageIdx++;
            curPage = allocatedPMemPages.get(curPageIdx);
            curOffsetInPage = 0;
            curNumOfRecInPage = 0;
        }

        @Override
        public Object getBaseObject() {
            return null;
        }

        @Override
        public long getBaseOffset() {
            return curRecordAddress;
        }

        @Override
        public int getRecordLength() {
            return recordLength;
        }

        @Override
        public long getKeyPrefix() {
            return keyPrefix;
        }

        @Override
        public int getNumRecords() {
            return numRecords;
        }
    }
}

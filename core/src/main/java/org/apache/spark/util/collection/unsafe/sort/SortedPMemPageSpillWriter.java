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
import org.apache.spark.unsafe.Platform;
import java.io.IOException;


public class SortedPMemPageSpillWriter extends UnsafeSorterPMemSpillWriter {
    private MemoryBlock currentPmemPage = null;
    private long currentOffsetInPage = 0L;
    private int currentNumOfRecordsInPage = 0;
    //Page -> record number map
    private HashMap<MemoryBlock,Integer> pageNumOfRecMap = new HashMap<MemoryBlock,Integer>();
    private int numRecords = 0;

    public SortedPMemPageSpillWriter(
            MemoryConsumer memConsumer,
            SortedIteratorForSpills sortedIterator,
            ShuffleWriteMetrics writeMetrics) {
        super(memConsumer, sortedIterator, writeMetrics);
    }

    //This write will write all spilled record in physically sorted PMem page.
    @Override
    public void write() throws IOException {
        while (sortedIterator.hasNext()) {
            sortedIterator.loadNext();
            final Object baseObject = sortedIterator.getBaseObject();
            final long baseOffset = sortedIterator.getBaseOffset();
            final int recordLength = sortedIterator.getRecordLength();
            if (allocatedPMemPages.isEmpty()) {
                currentPmemPage = allocatePMemPage();
                currentOffsetInPage = 0;
                currentNumOfRecordsInPage = 0;
            }
            long pageBaseOffset = currentPmemPage.getBaseOffset();
            long currentOffset = pageBaseOffset + currentOffsetInPage;
            if (currentOffset > pageBaseOffset + currentPmemPage.size()) {
                currentPmemPage = allocatePMemPage();
                currentOffsetInPage = 0;
                currentNumOfRecordsInPage = 0;
            }
            Platform.copyMemory(baseObject,baseOffset,null,currentOffset,recordLength);
            currentNumOfRecordsInPage ++;
            pageNumOfRecMap.put(currentPmemPage, currentNumOfRecordsInPage);
            numRecords ++;
        }
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
        private MemoryBlock currentPage = null;
        private int currentPageIdx = -1;
        private int currentOffsetInPage = 0;
        private int currentNumOfRecInPage = 0;
        private long currentRecordAddress = 0;
        private int recordLength;
        private long keyPrefix;
        private Object baseObject = null;

        public SortedPMemPageSpillReader() {

        }
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void loadNext() throws IOException {
            if (currentPage == null || currentNumOfRecInPage >= pageNumOfRecMap.get(currentPage)) {
                moveToNextPMemPage();
            }
            long recordAddress = currentPage.getBaseOffset + currentOffsetInPage;
            int uaoSize = UnsafeAlignedOffset.getUaoSize();
            recordLength = UnsafeAlignedOffset.getSize(null, recordAddress);
            currentOffsetInPage += recordLength;
            currentRecordAddress = uaoSize + recordAddress;
        }

        private void moveToNextPMemPage() {
            currentPageIdx ++;
            currentPage = allocatedPMemPages.get(currentPageIdx);
            currentOffsetInPage = 0;
            currentNumOfRecInPage = 0;
        }

        @Override
        public Object getBaseObject() {
            return baseObject;
        }

        @Override
        public long getBaseOffset() {
            return currentRecordAddress;
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

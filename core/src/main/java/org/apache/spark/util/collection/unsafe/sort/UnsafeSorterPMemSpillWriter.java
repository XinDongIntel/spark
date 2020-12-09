package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UnsafeSorterPMemSpillWriter implements SpillWriterForUnsafeSorter{
    private static final Logger logger = LoggerFactory.getLogger(UnsafeSorterPMemSpillWriter.class);
    /**
     * the memConsumer used to allocate pmem pages
     */
    protected UnsafeExternalSorter externalSorter;

    protected SortedIteratorForSpills sortedIterator;

    protected int numberOfRecordsToWritten = 0;

    protected TaskMemoryManager taskMemoryManager;

    //Todo: It's confusing to have ShuffleWriteMetrics here. will reconsider and fix it later.
    protected ShuffleWriteMetrics writeMetrics;

    protected TaskMetrics taskMetrics;

    protected LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<MemoryBlock>();

    //Page size in bytes.
    private static long DEFAULT_PAGE_SIZE = 64*1024*1024;

    public UnsafeSorterPMemSpillWriter(
        UnsafeExternalSorter externalSorter,
        SortedIteratorForSpills sortedIterator,
        int numberOfRecordsToWritten,
        ShuffleWriteMetrics writeMetrics,
        TaskMetrics taskMetrics) {
        this.externalSorter = externalSorter;
        this.taskMemoryManager = externalSorter.getTaskMemoryManager();
        this.sortedIterator = sortedIterator;
        this.numberOfRecordsToWritten = numberOfRecordsToWritten;
        this.writeMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
    }

    protected MemoryBlock allocatePMemPage() throws IOException { ;
        return allocatePMemPage(DEFAULT_PAGE_SIZE);
    }

    protected MemoryBlock allocatePMemPage(long size) {
        MemoryBlock page = taskMemoryManager.allocatePage(size, externalSorter, true);
        if(page != null) {
            allocatedPMemPages.add(page);
        }
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManager.freePage(page, externalSorter);
        }
        allocatedPMemPages.clear();
    }
}

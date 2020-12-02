package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.memory.MemoryBlock;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UnsafeSorterPMemSpillWriter implements SpillWriterForUnsafeSorter{
    protected static final Logger logger = LoggerFactory.getLogger(UnsafeSorterPMemSpillWriter.class);
    /**
     * the memConsumer used to allocate pmem pages
     */
    protected UnsafeExternalSorter externalSorter;

    protected SortedIteratorForSpills sortedIterator;

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
        ShuffleWriteMetrics writeMetrics,
        TaskMetrics taskMetrics) {
        this.externalSorter = externalSorter;
        this.taskMemoryManager = externalSorter.getTaskMemoryManager();
        this.sortedIterator = sortedIterator;
        this.writeMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
    }

    protected MemoryBlock allocatePMemPage() { ;
        return allocatePMemPage(DEFAULT_PAGE_SIZE);
    }

    protected MemoryBlock allocatePMemPage(long size) {
        logger.info("call allocate Pmem Page:{}",size);
        MemoryBlock page = taskMemoryManager.allocatePage(size, externalSorter, true);
        logger.info("PMem page allocated baseOffset:{}",page.getBaseOffset());
        allocatedPMemPages.add(page);
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManager.freePage(page, externalSorter);
            logger.info("free PMem Page:{}",page.getBaseOffset());
        }
        allocatedPMemPages.clear();
    }
}

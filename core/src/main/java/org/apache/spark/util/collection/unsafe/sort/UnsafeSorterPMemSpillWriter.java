package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.memory.MemoryBlock;
import java.util.LinkedList;

public abstract class UnsafeSorterPMemSpillWriter implements SpillWriterForUnsafeSorter{
    /**
     * the memConsumer used to allocate pmem pages
     */
    protected UnsafeExternalSorter externalSorter;

    protected SortedIteratorForSpills sortedIterator;

    protected TaskMemoryManager taskMemoryManager;

    protected ShuffleWriteMetrics shuffleWriteMetrics;

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
        this.shuffleWriteMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
    }

    protected MemoryBlock allocatePMemPage() {
        MemoryBlock page = taskMemoryManager.allocatePage(DEFAULT_PAGE_SIZE, memConsumer, true);
        allocatedPMemPages.add(page);
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManager.freePage(page, memConsumer);
        }
        allocatedPMemPages.clear();
    }
}

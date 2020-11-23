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
        MemoryBlock page = taskMemoryManager.allocatePage(size, externalSorter, true);
        allocatedPMemPages.add(page);
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManager.freePage(page, externalSorter);
        }
        allocatedPMemPages.clear();
    }
}

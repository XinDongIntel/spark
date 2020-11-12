package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.unsafe.memory.MemoryBlock;
import java.util.LinkedList;

public abstract class UnsafeSorterPMemSpillWriter implements SpillWriterForUnsafeSorter{
    /**
     * the memConsumer used to allocate pmem pages
     */
    protected MemoryConsumer memConsumer;

    protected SortedIteratorForSpills sortedIterator;

    protected TaskMemoryManager taskMemoryManager;

    protected ShuffleWriteMetrics shuffleWriteMetrics;

    protected LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<MemoryBloc>();

    private static long DEFAULT_PAGE_SIZE = 4*1024;

    public UnsafeSorterPMemSpillWriter(
        MemoryConsumer memConsumer,
        SortedIteratorForSpills sortedIterator,
        ShuffleWriteMetrics writeMetrics) {
        this.memConsumer = memConsumer;
        this.taskMemoryManager = memConsumer.getTaskMemoryManager();
        this.sortedIterator = sortedIterator;
        this.shuffleWriteMetrics = writeMetrics;
    }

    protected MemoryBlock allocatePMemPage() {
        MemoryBlock page = taskMemoryManager.allocatePage(DEFAULT_PAGE_SIZE, memConsumer, true);
        allocatedPMemPages.add(page);
        return page;
    }

    protected void freeAllPMemPages() {
        for (MemoryBlock page : allocatedPMemPages) {
            taskMemoryManger.freePage(page, memConsumer);
        }
        allocatedPMemPages.clear();
    }
}

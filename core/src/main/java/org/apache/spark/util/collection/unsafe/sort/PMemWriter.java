package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

public final class PMemWriter extends UnsafeSorterPMemSpillWriter {
    private final ShuffleWriteMetrics writeMetrics;
    private final TaskMemoryManager taskMemoryManager;
    private final LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<>();
    private LongArray sortedArray;
    private HashMap<MemoryBlock, MemoryBlock> pageMap = new HashMap<>();
    private int numRecordsWritten;
    private TaskMetrics taskMetrics;
    private int position;
    private LinkedList<MemoryBlock> allocatedPages;

    public PMemWriter(
        MemoryConsumer memoryConsumer,
        SortedIteratorForSpills sortedIterator, ShuffleWriteMetrics writeMetrics, TaskMetrics taskMetrics,
        TaskMemoryManager taskMemoryManager, int numRecords, LinkedList<MemoryBlock> allocatedPages) {
        super(memoryConsumer, sortedIterator, writeMetrics);
        this.writeMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
        this.taskMemoryManager = taskMemoryManager;
        this.numRecordsWritten = numRecords;
        this.allocatedPages = allocatedPages;
    }

    public boolean dumpPageToPMem(MemoryBlock page) {
        MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size());
        if (pMemBlock != null) {
            Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
            writeMetrics.incBytesWritten(page.size());
            allocatedPMemPages.add(pMemBlock);
            pageMap.put(page, pMemBlock);
            return true;
        }
        return false;
    }

    public int getNumRecordsWritten() {
        return numRecordsWritten;
    }

    public LinkedList<MemoryBlock> getAllocatedPMemPages() { return allocatedPMemPages; }

    public PMemReaderForUnsafeExternalSorter getPMemReaderForUnsafeExternalSorter() {
        return new PMemReaderForUnsafeExternalSorter(sortedArray, position, numRecordsWritten, taskMetrics);
    }

    @Override
    public void write() {
        long dumpTime = System.nanoTime();
        for (MemoryBlock page : allocatedPages) {
            dumpPageToPMem(page);
        }
        long dumpDuration = System.nanoTime() - dumpTime;
        System.out.println("dump time : " + dumpDuration / 1000000);
        long sortTime = System.nanoTime();
        updateLongArray(sortedIterator.getLongArray(), numRecordsWritten, 0);
        long sortDuration = System.nanoTime() - sortTime;
        System.out.println("sort time : " + sortDuration / 1000000);
    }

    @Override
    public UnsafeSorterIterator getSpillReader() {
        throw new RuntimeException("Unsupported operation");
    }

    public void clearAll() {
        if (getSortedArray().memoryBlock().pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) {
            memConsumer.freeArray(getSortedArray());
        }
        for (MemoryBlock block : getAllocatedPMemPages()) {
            freePMemPage(block);
        }
    }

    protected void freePMemPage(MemoryBlock page) {
        taskMemoryManager.freePMemPage(page, memConsumer);
    }

    public void updateLongArray(LongArray sortedArray, int numRecords, int position) {
        this.position = position;
        while (position < numRecords * 2){
            // update recordPointer in this array
            long originalRecordPointer = sortedArray.get(position);
            MemoryBlock page = taskMemoryManager.getOriginalPage(originalRecordPointer);
            long offset = taskMemoryManager.getOffsetInPage(originalRecordPointer) - page.getBaseOffset();
            MemoryBlock pMemBlock = pageMap.get(page);
            long pMemOffset = pMemBlock.getBaseOffset() + offset;
            sortedArray.set(position, pMemOffset);
            position += 2;
        }
        this.sortedArray = sortedArray;
    }

    public LongArray getSortedArray() {
        return sortedArray;
    }

    public int getNumOfSpilledRecords() { return numRecordsWritten - position/2; }


}

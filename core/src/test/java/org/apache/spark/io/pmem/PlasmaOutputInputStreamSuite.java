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
package org.apache.spark.io.pmem;

import org.apache.spark.shuffle.pmem.PlasmaBlockObjectWriter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.PlasmaShuffleBlockId;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assume.*;
import static org.junit.Assert.*;

/**
 * Tests functionality of {@link PlasmaInputStream} and {@link PlasmaOutputStream}
 */
public class PlasmaOutputInputStreamSuite extends PlasmaTestSuite {

  private final Random random = new Random();

  @BeforeClass
  public static void setUp() {
    boolean isAvailable =  isPlasmaJavaAvailable();
    assumeTrue("Please make sure libplasma_java.so is installed" +
        " under LD_LIBRARY_PATH or java.library.path", isAvailable);

    boolean isExist = isPlasmaStoreExist();
    assumeTrue("Please make sure plasma store server is installed " +
        "and added to the System PATH before run these unit tests", isExist);
    try {
      boolean isStarted = startPlasmaStore();
      assumeTrue("Failed to start plasma store server", isStarted);
    } catch (IOException ex1) {
      ex1.printStackTrace();
    } catch (InterruptedException ex2) {
      ex2.printStackTrace();
    }
    mockSparkEnv();
  }

  @Test(expected = NullPointerException.class)
  public void testWithNullData() throws IOException {
    PlasmaOutputStream pos = new PlasmaOutputStream("testWithEmptyData-dummy-id");
    pos.write(null);
  }

  @Test
  public void testBufferWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(1);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    byte[] bytesRead = new byte[bytesWrite.length];
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    pis.read(bytesRead);
    assertArrayEquals(bytesWrite, bytesRead);

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testPartialBlockWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2.7);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int len;
    while ((len = pis.read(buffer)) != -1) {
      bytesRead.put(buffer, 0, len);
    }
    assertArrayEquals(bytesWrite, bytesRead.array());

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testMultiBlocksWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    while (pis.read(buffer) != -1) {
      bytesRead.put(buffer);
    }
    assertArrayEquals(bytesWrite, bytesRead.array());

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testMultiThreadReadWrite() throws InterruptedException {
    int processNum = Runtime.getRuntime().availableProcessors();
    ExecutorService threadPool = Executors.newFixedThreadPool(processNum);
    for (int i = 0; i < 10 * processNum; i++) {
      threadPool.submit(() -> {
        try {
          String blockId = "block_id_" + random.nextInt(10000000);
          byte[] bytesWrite = prepareByteBlockToWrite(5.7);
          PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
          pos.write(bytesWrite);

          ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
          PlasmaInputStream pis = new PlasmaInputStream(blockId);
          byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
          int len;
          while ((len = pis.read(buffer)) != -1) {
            bytesRead.put(buffer, 0, len);
          }
          assertArrayEquals(bytesWrite, bytesRead.array());

          PlasmaUtils.remove(blockId);
          assertFalse(PlasmaUtils.contains(blockId));
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
  }

  @Test
  public void testPlasmaObjectWriter() throws IOException {
    BlockId blockId = new PlasmaShuffleBlockId(0, 0, 0);
    PlasmaBlockObjectWriter writer = createWriter(blockId);
    writer.write("key1", "value1");
    // TODO: incomplete due to no append feature in plasma output stream
  }

  @AfterClass
  public static void tearDown() {
    try {
      MyPlasmaClientHolder.close();
      stopPlasmaStore();
      deletePlasmaSocketFile();
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }

  private byte[] prepareByteBlockToWrite(double numOfBlock) {
    byte[] bytesToWrite = new byte[(int) (DEFAULT_BUFFER_SIZE * numOfBlock)];
    random.nextBytes(bytesToWrite);
    return bytesToWrite;
  }
}

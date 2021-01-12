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

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assume.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests functionality of {@link PlasmaInputStream} and {@link PlasmaOutputStream}
 */
public class PlasmaOutputInputStreamSuite {

  private final static int DEFAULT_BUFFER_SIZE = 4096;

  private final static String plasmaStoreServer = "plasma-store-server";
  private final static String plasmaStoreSocket = "/tmp/PlasmaOutputInputStreamSuite_socket_file";
  private final static long memoryInBytes = 1000000000;

  private static Process process;
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

  public static boolean isPlasmaJavaAvailable() {
    boolean available = true;
    try {
      System.loadLibrary("plasma_java");
    } catch (UnsatisfiedLinkError ex) {
      available = false;
    }
    return available;
  }

  private static boolean isPlasmaStoreExist() {
    return Stream.of(System.getenv("PATH").split(Pattern.quote(File.pathSeparator)))
        .map(Paths::get)
        .anyMatch(path -> Files.exists(path.resolve(plasmaStoreServer)));
  }

  private static Process startProcess(String command) throws IOException {
    List<String> cmdList = Arrays.stream(command.split(" ")).collect(Collectors.toList());
    ProcessBuilder processBuilder = new ProcessBuilder(cmdList).inheritIO();
    Process process = processBuilder.start();
    return process;
  }

  private static boolean startPlasmaStore() throws IOException, InterruptedException {
    String command = plasmaStoreServer + " -s " + plasmaStoreSocket + " -m " + memoryInBytes;
    process = startProcess(command);
    int ticktock = 60;
    if (process != null) {
      while(!process.isAlive()) {
        TimeUnit.MILLISECONDS.sleep(1000);
        ticktock--;
        if (ticktock == 0 && !process.isAlive()) {
          throw new RuntimeException("Failed to start plasma store server");
        }
      }
      return true;
    }
    return false;
  }

  private static void stopPlasmaStore() throws InterruptedException {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      int ticktock = 60;
      while (process.isAlive()) {
        TimeUnit.MILLISECONDS.sleep(1000);
        ticktock--;
        if (ticktock == 0 && process.isAlive()) {
          throw new RuntimeException("Failed to stop plasma store server");
        }
      }
    }
  }

  private static void deletePlasmaSocketFile() {
    File socketFile = new File(plasmaStoreSocket);
    if (socketFile != null && socketFile.exists()) {
      socketFile.delete();
    }
  }

  private byte[] prepareByteBlockToWrite(double numOfBlock) {
    byte[] bytesToWrite = new byte[(int) (DEFAULT_BUFFER_SIZE * numOfBlock)];
    random.nextBytes(bytesToWrite);
    return bytesToWrite;
  }

  private static void mockSparkEnv() {
    SparkConf conf = new SparkConf();
    conf.set("spark.io.plasma.server.socket", plasmaStoreSocket);
    SparkEnv mockEnv = mock(SparkEnv.class);
    SparkEnv.set(mockEnv);
    when(mockEnv.conf()).thenReturn(conf);
  }
}

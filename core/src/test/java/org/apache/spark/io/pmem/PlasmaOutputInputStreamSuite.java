package org.apache.spark.io.pmem;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assume.*;
import static org.junit.Assert.*;

/**
 * Tests functionality of {@link PlasmaInputStream} and {@link PlasmaOutputStream}
 */
public class PlasmaOutputInputStreamSuite {

  private final static int DEFAULT_BUFFER_SIZE = 4096;

  private final String plasmaStoreServer = "plasma-store-server";
  private final String plasmaStoreSocket = "/tmp/PlasmaOutputInputStreamSuite_socket_file";
  private final long memoryInBytes = 1000000000;

  private Process process;
  private final Random random = new Random();

  @Before
  public void setUp() {
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
  }

  @Test(expected = NullPointerException.class)
  public void testWithNullData() throws IOException {
    PlasmaOutputStream pos = new PlasmaOutputStream(
        "testWithEmptyData-dummy-id", plasmaStoreSocket);
    pos.write(null);
  }

  @Test
  public void testSingleWriteRead() {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(1);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId, plasmaStoreSocket);
    for (byte b : bytesWrite) {
      pos.write(b);
    }
    pos.close();

    byte[] bytesRead = new byte[bytesWrite.length];
    PlasmaInputStream pis = new PlasmaInputStream(blockId, plasmaStoreSocket);
    for (int i = 0; i < bytesRead.length; i++) {
      bytesRead[i] = (byte) pis.read();
    }
    pis.close();

    assertArrayEquals(bytesWrite, bytesRead);
  }

  @Test
  public void testBufferWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(1);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId, plasmaStoreSocket);
    pos.write(bytesWrite);
    pos.close();

    byte[] bytesRead = new byte[bytesWrite.length];
    PlasmaInputStream pis = new PlasmaInputStream(blockId, plasmaStoreSocket);
    pis.read(bytesRead);
    pis.close();
    assertArrayEquals(bytesWrite, bytesRead);
  }

  @Test
  public void testPartialBlockWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2.7);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId, plasmaStoreSocket);
    pos.write(bytesWrite);
    pos.close();

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId, plasmaStoreSocket);
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int len;
    while ((len = pis.read(buffer)) != -1) {
      bytesRead.put(buffer, 0, len);
    }
    pis.close();

    assertArrayEquals(bytesWrite, bytesRead.array());
  }

  @Test
  public void testMultiBlocksWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId, plasmaStoreSocket);
    pos.write(bytesWrite);
    pos.close();

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId, plasmaStoreSocket);
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    while (pis.read(buffer) != -1) {
      bytesRead.put(buffer);
    }
    pis.close();

    assertArrayEquals(bytesWrite, bytesRead.array());
  }

  @After
  public void tearDown() {
    try {
      stopPlasmaStore();
      deletePlasmaSocketFile();
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }

  public boolean isPlasmaJavaAvailable() {
    boolean available = true;
    try {
      System.loadLibrary("plasma_java");
    } catch (UnsatisfiedLinkError ex) {
      available = false;
    }
    return available;
  }

  private boolean isPlasmaStoreExist() {
    return Stream.of(System.getenv("PATH").split(Pattern.quote(File.pathSeparator)))
        .map(Paths::get)
        .anyMatch(path -> Files.exists(path.resolve(plasmaStoreServer)));
  }

  private Process startProcess(String command) throws IOException {
    List<String> cmdList = Arrays.stream(command.split(" ")).collect(Collectors.toList());
    ProcessBuilder processBuilder = new ProcessBuilder(cmdList).inheritIO();
    Process process = processBuilder.start();
    return process;
  }

  private boolean startPlasmaStore() throws IOException, InterruptedException {
    String command = plasmaStoreServer + " -s " + plasmaStoreSocket + " -m " + memoryInBytes;
    this.process = startProcess(command);
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

  private void stopPlasmaStore() throws InterruptedException {
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

  private void deletePlasmaSocketFile() {
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
}

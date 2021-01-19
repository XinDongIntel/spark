package org.apache.spark.io.pmem;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.pmem.PlasmaBlockObjectWriter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.PlasmaShuffleBlockId;
import org.glassfish.jersey.message.internal.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlasmaTestSuite {

  public final static int DEFAULT_BUFFER_SIZE = 4096;

  public final static String plasmaStoreServer = "plasma-store-server";
  public final static String plasmaStoreSocket = "/tmp/PlasmaOutputInputStreamSuite_socket_file";
  public final static long memoryInBytes = 1000000000;

  public static Process process;

  public static boolean isPlasmaJavaAvailable() {
    boolean available = true;
    try {
      System.loadLibrary("plasma_java");
    } catch (UnsatisfiedLinkError ex) {
      available = false;
    }
    return available;
  }

  public static boolean isPlasmaStoreExist() {
    return Stream.of(System.getenv("PATH").split(Pattern.quote(File.pathSeparator)))
        .map(Paths::get)
        .anyMatch(path -> Files.exists(path.resolve(plasmaStoreServer)));
  }

  public static Process startProcess(String command) throws IOException {
    List<String> cmdList = Arrays.stream(command.split(" ")).collect(Collectors.toList());
    ProcessBuilder processBuilder = new ProcessBuilder(cmdList).inheritIO();
    Process process = processBuilder.start();
    return process;
  }

  public static boolean startPlasmaStore() throws IOException, InterruptedException {
    String command = plasmaStoreServer + " -s " + plasmaStoreSocket + " -m " + memoryInBytes;
    process = startProcess(command);
    int ticktock = 60;
    if (process != null) {
      while (!process.isAlive()) {
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

  public static void stopPlasmaStore() throws InterruptedException {
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

  public static void deletePlasmaSocketFile() {
    File socketFile = new File(plasmaStoreSocket);
    if (socketFile != null && socketFile.exists()) {
      socketFile.delete();
    }
  }

  public static void mockSparkEnv() {
    SparkConf conf = new SparkConf();
    conf.set("spark.io.plasma.server.socket", plasmaStoreSocket);
    SparkEnv mockEnv = mock(SparkEnv.class);
    SparkEnv.set(mockEnv);
    when(mockEnv.conf()).thenReturn(conf);
  }

  public PlasmaBlockObjectWriter createWriter(BlockId blockId) throws IOException {
    SparkConf conf = new SparkConf();
    JavaSerializer javaSerializer = new JavaSerializer(conf);
    PlasmaBlockObjectWriter objectWriter = new PlasmaBlockObjectWriter(
        new SerializerManager(javaSerializer, conf),
        javaSerializer.newInstance(),
        blockId,
        new File(Utils.createTempFile(), "spilling_file"));
    return objectWriter;
  }
}

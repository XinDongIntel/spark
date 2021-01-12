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

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class PlasmaMemoryManagerSuite {

  @Test
  public void testMultiThreadUseCase() throws InterruptedException {
    PlasmaMemoryManager memoryManager = new PlasmaMemoryManager(100000000, 0.8);
    int processNum = Runtime.getRuntime().availableProcessors();
    ExecutorService threadPool = Executors.newFixedThreadPool(processNum);
    for (int i = 0; i < 100 * processNum; i++) {
      threadPool.submit(() -> {
        memoryManager.increase(100);
        memoryManager.release(100);
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    assertEquals(0, memoryManager.memoryUsed.get());
  }

}

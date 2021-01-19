/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.io.pmem;

import java.nio.ByteBuffer;

/**
 * Utility to operate object stored in plasma server
 */
public class PlasmaUtils {

  private static MyPlasmaClient client = MyPlasmaClientHolder.get();
  public static final int DEFAULT_BUFFER_SIZE = 4096;

  public static boolean contains(String objectId) {
    int num = client.getChildObjectNumber(objectId);
    return num > 0;
  }

  public static void remove(String objectId) {
    int num = client.getChildObjectNumber(objectId);
    for (int i = 0; i < num; i++) {
      ChildObjectId childObjectId = new ChildObjectId(objectId, i);
      client.release(childObjectId.toBytes());
      client.delete(childObjectId.toBytes());
    }
    client.delete(client.paddingParentObjectId(objectId).getBytes());
  }

  public static ByteBuffer getObjAsByteBuffer(String objectId) {
    int num = client.getChildObjectNumber(objectId);
    int len = client.getLastChildObjectLen(objectId);
    int totalBufferSize;
    if (len > 0) {
      totalBufferSize = ((num - 1) * DEFAULT_BUFFER_SIZE) + len;
    } else {
      totalBufferSize = num * DEFAULT_BUFFER_SIZE;
    }
    ByteBuffer buffer = ByteBuffer.allocate(totalBufferSize);
    for (int i = 0; i < num; i++) {
      ByteBuffer buf = client.getChildObject(objectId, i);
      buffer.put(buf);
    }
    return buffer;
  }
}

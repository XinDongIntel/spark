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

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkEnv;
import org.apache.spark.internal.config.package$;

import java.nio.ByteBuffer;

/**
 * Upstream Plasma Client Wrapper.
 * To simplify the parameter passed to plasma client.
 * Make finalize() in PlasmaClient can be invoked explicitly.
 * This class should be remove in the future.
 */
public class MyPlasmaClient extends PlasmaClient {

  public MyPlasmaClient(String storeSocketName) {
    super(storeSocketName, "", 0);
  }

  /**
   * Write to plasma store with an child object id
   */
  public void writeChildObject(String parentObjectId, int index, byte[] buffer) {
    ChildObjectId objectId = new ChildObjectId(parentObjectId, index);
    put(objectId.toBytes(), buffer, null);
  }

  public ByteBuffer getChildObject(String parentObjectId, int index) {
    ChildObjectId childObjectId = new ChildObjectId(parentObjectId, index);
    ByteBuffer buffer = getObjAsByteBuffer(childObjectId.toBytes(), 0, false);
    return buffer;
  }

  public int getChildObjectNumber(String parentObjectId) {
    ByteBuffer buffer = getObjAsByteBuffer(paddingParentObjectId(parentObjectId).getBytes(),
        0, false);
    if (buffer == null) {
      return -1;
    }
    ChildObjectMetaData metaData = new ChildObjectMetaData(buffer);
    release(paddingParentObjectId(parentObjectId).getBytes());
    return metaData.getChildObjectNum();
  }

  String paddingParentObjectId(String parentObjectId) {
    // Padding with - to prevent duplicate from child object id.
    return StringUtils.rightPad(parentObjectId, 20, "-");
  }

  @Override
  public void finalize() {
    super.finalize();
  }
}

class ChildObjectId {

  private final String objectId;

  public ChildObjectId(String parentObjectId, int index) {
    int parentObjectIdLen = parentObjectId.length();
    int indexDigitNum = String.valueOf(index).length();

    if (parentObjectIdLen + indexDigitNum > 20) {
      throw new IllegalArgumentException("Each object in the Plasma store" +
          " should be associated with a unique ID which is a string of 20 length");
    } else {
      this.objectId = StringUtils.rightPad(parentObjectId, 20 - indexDigitNum, "0") + index;
    }
  }

  public byte[] toBytes() {
    return objectId.getBytes();
  }
}

/**
 * Use 8 bytes to record child object metadata.
 * The first 4 bytes will record the total number of child objects.
 * The last 4 bytes will record the length of last child object.
 */
class ChildObjectMetaData {
  ByteBuffer buf;

  public ChildObjectMetaData(int num, int len) {
    buf = ByteBuffer.allocate(8);
    buf.putInt(num);
    buf.putInt(len);
  }

  public ChildObjectMetaData(ByteBuffer buf) {
    this.buf = buf;
  }

  public int getChildObjectNum() {
    return buf.getInt(0);
  }

  public int getChildObjectLen() {
    return buf.getInt(4);
  }

  public byte[] getBytes() {
    return buf.array();
  }

}

/**
 * Hold a global plasma client instance.
 */
class MyPlasmaClientHolder {

  private static MyPlasmaClient client;
  private static String DEFAULT_STORE_SERVER_SOCKET = "/tmp/plasma";

  static {
    System.loadLibrary("plasma_java");
  }

  private MyPlasmaClientHolder() {}

  public static MyPlasmaClient get() {
    if (client == null) {
      String storeSocketName = SparkEnv.get() == null ? DEFAULT_STORE_SERVER_SOCKET :
          SparkEnv.get().conf().get(package$.MODULE$.PLASMA_SERVER_SOCKET());
      client = new MyPlasmaClient(storeSocketName);
    }
    return client;
  }

  public static void close() {
    if (client != null) {
      client.finalize();
      client = null;
    }
  }
}

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

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is customized output stream implementation which utilizes
 * arrow plasma object store when writing data.
 * The parent object id will be used as the key to store child objects metadata
 * The child object id will be used as the key to store each fixed-length object
 */
public class PlasmaOutputStream extends OutputStream {

  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final String parentObjectId;
  private final MyPlasmaClient client;

  private int currChildObjectNumber;
  private final ByteBuffer buffer;

  /**
   * Initialize output stream with a parent object id.
   * Initialize plasma client.
   * Initialize with a customize buffer size.
   *
   * @param parentObjectId  parent object id
   * @param storeSocketName plasma object store socket
   * @param bufferSize      buffer size
   */
  public PlasmaOutputStream(String parentObjectId, String storeSocketName, int bufferSize) {
    if (bufferSize < 0) {
      throw new IllegalArgumentException("buffer size can not be a negative number");
    }
    this.buffer = ByteBuffer.allocate(bufferSize);
    this.parentObjectId = parentObjectId;
    this.client = new MyPlasmaClient(storeSocketName);
    this.currChildObjectNumber = 0;
  }

  /**
   * Use {@code DEFAULT_BUFFER_SIZE} as buffer size.
   *
   * @param parentObjectId
   * @param storeSocketName
   */
  public PlasmaOutputStream(String parentObjectId, String storeSocketName) {
    this(parentObjectId, storeSocketName, DEFAULT_BUFFER_SIZE);
  }

  @Override
  public void write(int b) {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      writeToPlasma();
      buffer.clear();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    int bytesToWrite = len;
    while (bytesToWrite > 0) {
      int remainBytesInBuf = buffer.remaining();
      if (remainBytesInBuf <= bytesToWrite) {
        buffer.put(b, off, remainBytesInBuf);
        bytesToWrite -= remainBytesInBuf;
        off += remainBytesInBuf;
      } else {
        buffer.put(b, off, bytesToWrite);
        off += bytesToWrite;
        bytesToWrite = 0;
      }
      writeToPlasma();
      buffer.clear();
      currChildObjectNumber++;
    }
  }

  private void writeToPlasma() {
    if (buffer.hasRemaining()) {
      client.write(parentObjectId, currChildObjectNumber, shrinkLastObjBuffer());
    } else {
      client.write(parentObjectId, currChildObjectNumber, buffer.array());
    }
  }

  private byte[] shrinkLastObjBuffer() {
    byte[] lastObjBytes = new byte[buffer.position()];
    buffer.flip();
    buffer.get(lastObjBytes);
    return lastObjBytes;
  }

  @Override
  public void close() {
    client.finalize();
  }
}

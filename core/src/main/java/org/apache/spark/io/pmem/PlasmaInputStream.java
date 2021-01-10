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

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This class is customized input stream implementation
 * which read data from arrow plasma object
 */
public class PlasmaInputStream extends InputStream {

  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final String parentObjectId;
  private final MyPlasmaClient client;

  private int bufferSize;
  private int currChildObjectNumber;
  private ByteBuffer buffer;

  /**
   * Initialize output stream with a parent object id.
   * Initialize plasma client.
   * Make sure the given buffer size for input stream is equal to the output stream's
   *
   * @param parentObjectId  parent object id
   * @param bufferSize buffer size
   */
  public PlasmaInputStream(String parentObjectId, int bufferSize) {
    this.bufferSize = bufferSize;
    this.buffer = ByteBuffer.allocate(bufferSize);
    buffer.flip();

    this.parentObjectId = parentObjectId;
    this.client = MyPlasmaClientHolder.get();
    this.currChildObjectNumber = 0;
  }

  /**
   * Use {@code DEFAULT_BUFFER_SIZE} as buffer size.
   *
   * @param parentObjectId
   */
  public PlasmaInputStream(String parentObjectId) {
    this(parentObjectId, DEFAULT_BUFFER_SIZE);
  }

  private boolean refill() {
    if (!buffer.hasRemaining()) {
      buffer.clear();
      ByteBuffer bufFromPlasma = client.getChildObject(parentObjectId, currChildObjectNumber++);
      if (bufFromPlasma == null) {
        return false;
      }
      buffer.put(bufFromPlasma);
      buffer.flip();
    }
    return true;
  }

  @Override
  public int read() {
    throw new UnsupportedOperationException("The method is not implemented");
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    if (!refill()) {
      return -1;
    }
    len = Math.min(len, buffer.remaining());
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public int available() {
    return buffer.remaining();
  }

  @Override
  public long skip(long n) {
    long remaining = n;
    int nr;

    if (n <= 0) {
      return 0;
    }

    int size = (int) Math.min(bufferSize, remaining);
    byte[] skipBuffer = new byte[size];
    while (remaining > 0) {
      nr = read(skipBuffer, 0, (int) Math.min(size, remaining));
      if (nr < 0) {
        break;
      }
      remaining -= nr;
    }
    return n - remaining;
  }

}

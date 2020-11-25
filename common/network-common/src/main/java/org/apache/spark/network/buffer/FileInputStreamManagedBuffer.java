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

package org.apache.spark.network.buffer;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.util.TransportConf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Enumeration;
import java.io.*;

public class FileInputStreamManagedBuffer extends ManagedBuffer{
    private final TransportConf conf;
    private List<FileInputStream> streams = new ArrayList<FileInputStream>();
    private int curStreamIdx = 0;
    private long totalLength = 0L;

    public FileInputStreamManagedBuffer(TransportConf conf) {
        this.conf = conf;
    }

    @Override
    public long size() {
        return totalLength;
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
    //Todo
        return null;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        Enumeration<FileInputStream> enmStreams = Collections.enumeration(streams);
        SequenceInputStream resultStream = new SequenceInputStream(enmStreams);
        return resultStream;
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
       //Will implements this later.
        return null;
    }

    public void addStream(FileInputStream stream) throws IOException {
        streams.add(stream);
        totalLength += stream.available();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .toString();
    }

}

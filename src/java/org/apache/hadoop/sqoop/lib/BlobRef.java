/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 * BlobRef is a wrapper that holds a Blob either directly, or a
 * reference to a file that holds the blob data.
 */
public class BlobRef implements Writable {

  public BlobRef(byte [] bytes) {
    this.blobFileNum = 0;
    this.data = new BytesWritable(bytes);
  }

  public BlobRef() {
    this.blobFileNum = 0;
    this.data = null;
  }

  // If the data is 'small', it's held directly, here.
  private BytesWritable data;

  // If there data is too large, it's written into a file
  // and the file is numbered; this number is recorded here.
  // This takes precedence if this value is positive.
  private long blobFileNum;

  public byte [] getData() {
    if (blobFileNum > 0) {
      // We have a numbered file.
      // TODO: Implement this.
      throw new RuntimeException("Unsupported: Indirect BLOBs are not supported");
    }

    return data.getBytes();
  }

  @Override
  public String toString() {
    if (blobFileNum > 0) {
      return "indirectBlob(" + blobFileNum + ")";
    } else {
      return data.toString();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // The serialization format for this object is:
    // boolean isIndirect
    // if true, the next field is a Long containing blobFileNum
    // if false, the next field is String data.

    boolean isIndirect = in.readBoolean();
    if (isIndirect) {
      this.data = null;
      this.blobFileNum = in.readLong();
    } else {
      if (null == this.data) {
        this.data = new BytesWritable();
      }
      this.data.readFields(in);
      this.blobFileNum = 0;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean isIndirect = blobFileNum > 0;
    out.writeBoolean(isIndirect);
    if (isIndirect) {
      out.writeLong(blobFileNum);
    } else {
      data.write(out);
    }
  }
}


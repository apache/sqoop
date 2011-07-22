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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * ClobRef is a wrapper that holds a Clob either directly, or a
 * reference to a file that holds the clob data.
 */
public class ClobRef implements Writable {

  public ClobRef(String chars) {
    this.clobFileNum = 0;
    this.data = chars;
  }

  public ClobRef() {
    this.clobFileNum = 0;
    this.data = null;
  }

  // If the data is 'small', it's held directly, here.
  private String data;

  // If there data is too large, it's written into a file
  // and the file is numbered; this number is recorded here.
  // This takes precedence if this value is positive.
  private long clobFileNum;

  public String getData() {
    if (clobFileNum > 0) {
      // We have a numbered file.
      // TODO: Implement this.
      throw new RuntimeException("Unsupported: Indirect CLOBs are not supported");
    }

    return data;
  }

  @Override
  public String toString() {
    if (clobFileNum > 0) {
      return "indirectClob(" + clobFileNum + ")";
    } else {
      return data;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // The serialization format for this object is:
    // boolean isIndirect
    // if true, the next field is a Long containing clobFileNum
    // if false, the next field is String data.

    boolean isIndirect = in.readBoolean();
    if (isIndirect) {
      this.data = null;
      this.clobFileNum = in.readLong();
    } else {
      this.data = Text.readString(in);
      this.clobFileNum = 0;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    boolean isIndirect = clobFileNum > 0;
    out.writeBoolean(isIndirect);
    if (isIndirect) {
      out.writeLong(clobFileNum);
    } else {
      Text.writeString(out, data);
    }
  }
}


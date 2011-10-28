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
package org.apache.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Serialize LOB classes to/from DataInput and DataOutput objects.
 */
public final class LobSerializer {

  private LobSerializer() { }

  public static void writeClob(
      com.cloudera.sqoop.lib.ClobRef clob, DataOutput out) throws IOException {
    clob.write(out);
  }

  public static void writeBlob(
      com.cloudera.sqoop.lib.BlobRef blob, DataOutput out) throws IOException {
    blob.write(out);
  }

  public static com.cloudera.sqoop.lib.ClobRef readClobFields(
      DataInput in) throws IOException {
    com.cloudera.sqoop.lib.ClobRef clob = new com.cloudera.sqoop.lib.ClobRef();
    clob.readFields(in);
    return clob;
  }

  public static com.cloudera.sqoop.lib.BlobRef readBlobFields(
      DataInput in) throws IOException {
    com.cloudera.sqoop.lib.BlobRef blob = new com.cloudera.sqoop.lib.BlobRef();
    blob.readFields(in);
    return blob;
  }
}

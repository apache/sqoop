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
package com.cloudera.sqoop.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;

/**
 * Serialize LOB classes to/from DataInput and DataOutput objects.
 *
 * @deprecated use org.apache.sqoop.lib.LobSerializer instead.
 * @see org.apache.sqoop.lib.LobSerializer
 */
public final class LobSerializer {

  private LobSerializer() { }

  public static void writeClob(ClobRef clob, DataOutput out)
      throws IOException {
    org.apache.sqoop.lib.LobSerializer.writeClob(clob, out);
  }

  public static void writeBlob(BlobRef blob, DataOutput out)
      throws IOException {
    org.apache.sqoop.lib.LobSerializer.writeBlob(blob, out);
  }

  public static ClobRef readClobFields(DataInput in) throws IOException {
    return org.apache.sqoop.lib.LobSerializer.readClobFields(in);
  }

  public static BlobRef readBlobFields(DataInput in) throws IOException {
    return org.apache.sqoop.lib.LobSerializer.readBlobFields(in);
  }
}

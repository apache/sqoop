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

package org.apache.sqoop.manager.oracle.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Connection;

/**
 * Generates Blob test data.
 */
public class BlobGenerator extends OraOopTestDataGenerator<Blob> {
  private static Class<?> blobClass;
  private static Method methCreateTemporary;
  private static Method methGetBufferSize;
  private static int durationSession;

  static {
    try {
      blobClass = Class.forName("oracle.sql.BLOB");
      methCreateTemporary =
          blobClass.getMethod("createTemporary", Connection.class,
              boolean.class, int.class);
      methGetBufferSize = blobClass.getMethod("getBufferSize");
      durationSession = blobClass.getField("DURATION_SESSION").getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(
          "Problem getting Oracle JDBC methods via reflection.", e);
    }
  }

  private Connection conn;
  private int minBytes;
  private int maxBytes;

  /**
   * Create a generator that will generate BLOBs with length varying between
   * minBytes and maxBytes.
   *
   * @param conn
   *          Oracle connection to use when creating BLOBs
   * @param minBytes
   *          Minimum number of bytes in generated BLOBs
   * @param maxBytes
   *          Maximum number of bytes in generated BLOBs
   */
  public BlobGenerator(Connection conn, int minBytes, int maxBytes) {
    super();
    this.conn = conn;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;
  }

  @Override
  public Blob next() {
    try {
      Blob blob =
          (Blob) methCreateTemporary.invoke(null, conn, false, durationSession);

      int blobSize =
          (int) (rng.nextDouble() * (maxBytes - minBytes) + minBytes);
      byte[] blobData = new byte[blobSize];
      rng.nextBytes(blobData);

      // blob.setBytes(blobData);

      OutputStream os = blob.setBinaryStream(1);
      InputStream is = new ByteArrayInputStream(blobData);
      int bufferSize = (Integer) methGetBufferSize.invoke(blob);
      byte[] buffer = new byte[bufferSize];
      int bytesRead = 0;
      while ((bytesRead = is.read(buffer)) != -1) {
        os.write(buffer, 0, bytesRead);
      }
      os.close();
      is.close();

      return blob;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}

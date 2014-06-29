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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility methods for OraOop system tests.
 */
public final class OraOopTestUtils {
  private OraOopTestUtils() {
  }
  /**
   * Pipe data from an input stream to an output stream in a separate thread.
   *
   * @param in
   *          Stream to pipe data from
   * @param out
   *          Stream to pipe data to
   * @return The thread in which data is being piped.
   */
  public static Thread backgroundPipe(final InputStream in,
      final OutputStream out) {
    Thread pipe = new Thread() {
      @Override
      public void run() {
        try {
          byte[] buffer = new byte[10 * 1024];
          int len;
          while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
          }
          out.flush();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    pipe.start();
    return pipe;
  }
}

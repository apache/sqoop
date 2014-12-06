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
package org.apache.sqoop.common.test.utils;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Network related utilities.
 */
public class NetworkUtils {

  /**
   * Create port number that is available on this machine for
   * subsequent use.
   *
   * Please note that this method doesn't guarantee that you
   * will be able to subsequently use that port as there is
   * a race condition when this method can return port number
   * that is available at the time of the call, but will be
   * used by different process before the caller will have a
   * chance to use it.
   *
   * This is merely a helper method to find a random available
   * port on the machine to enable multiple text executions
   * on the same machine or deal with situation when some tools
   * (such as Derby) do need specific port and can't be started
   * on "random" port.
   *
   * @return Available port number
   * @throws IOException
   */
  public static int findAvailablePort() throws IOException {
    ServerSocket socket = null;

    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if(socket != null) {
        socket.close();
      }
    }
  }

  private NetworkUtils() {
    // Instantiation is prohibited
  }
}

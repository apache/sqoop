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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

/**
 * Network related utilities.
 */
public class NetworkUtils {

  private static final Logger LOG = Logger.getLogger(NetworkUtils.class);

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

  /**
   * Create a socket and attempt to connect to ``hostname``:``port``
   * ``numberOfAttempts`` amount of times with ``sleepTime`` milliseconds
   * between each attempt.
   *
   * @param hostname host name to connect to
   * @param port port to connect on
   * @param numberOfAttempts number of tries to connect
   * @param sleepTime time in between connection attempts in milliseconds
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public static void waitForStartUp(String hostname, int port, int numberOfAttempts, long sleepTime)
      throws InterruptedException, TimeoutException {
    for (int i = 0; i < numberOfAttempts; ++i) {
      try {
        LOG.debug("Attempt " + (i + 1) + " to access " + hostname + ":" + port);
        new Socket(InetAddress.getByName(hostname), port).close();
        return;
      } catch (Exception e) {
        LOG.debug("Failed to connect to " + hostname + ":" + port, e);
      }

      Thread.sleep(sleepTime);
    }

    throw new TimeoutException("Couldn't access new server: " + hostname + ":" + port);
  }

  private NetworkUtils() {
    // Instantiation is prohibited
  }
}

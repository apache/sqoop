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

package com.cloudera.sqoop.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utilities for mysql-based tests.
 */
public final class MySQLTestUtils {

  public static final Log LOG = LogFactory.getLog(
      MySQLTestUtils.class.getName());

  public static final String HOST_URL = System.getProperty(
      "sqoop.test.mysql.connectstring.host_url",
      "jdbc:mysql://localhost/");

  public static final String MYSQL_DATABASE_NAME = "sqooptestdb";
  public static final String TABLE_NAME = "EMPLOYEES_MYSQL";
  public static final String CONNECT_STRING = HOST_URL + MYSQL_DATABASE_NAME;

  private MySQLTestUtils() { }

  /** @return the current username. */
  public static String getCurrentUser() {
    // First, check the $USER environment variable.
    String envUser = System.getenv("USER");
    if (null != envUser) {
      return envUser;
    }

    // Try `whoami`
    String [] whoamiArgs = new String[1];
    whoamiArgs[0] = "whoami";
    Process p = null;
    BufferedReader r = null;
    try {
      p = Runtime.getRuntime().exec(whoamiArgs);
      InputStream is = p.getInputStream();
      r = new BufferedReader(new InputStreamReader(is));
      return r.readLine();
    } catch (IOException ioe) {
      LOG.error("IOException reading from `whoami`: " + ioe.toString());
      return null;
    } finally {
      // close our stream.
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing input stream from `whoami`: "
              + ioe.toString());
        }
      }

      // wait for whoami to exit.
      while (p != null) {
        try {
          int ret = p.waitFor();
          if (0 != ret) {
            LOG.error("whoami exited with error status " + ret);
            // suppress original return value from this method.
            return null;
          }
        } catch (InterruptedException ie) {
          continue; // loop around.
        }
      }
    }
  }
}

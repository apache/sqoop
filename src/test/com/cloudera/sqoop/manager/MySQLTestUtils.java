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

import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Utilities for mysql-based tests.
 */
public final class MySQLTestUtils {

  public static final Log LOG = LogFactory.getLog(
      MySQLTestUtils.class.getName());

  private String hostUrl;

  private String userName;
  private String userPass;

  private String mysqlDbNAme;
  private String mySqlConnectString;

  public MySQLTestUtils() {
    hostUrl = System.getProperty(
        "sqoop.test.mysql.connectstring.host_url",
        "jdbc:mysql://localhost/");
    userName = System.getProperty("sqoop.test.mysql.username", getCurrentUser());
    userPass = System.getProperty("sqoop.test.mysql.password");

    mysqlDbNAme = System.getProperty("sqoop.test.mysql.databasename", "sqooptestdb");
    mySqlConnectString = getHostUrl() + getMysqlDbNAme();
  }

  public String getHostUrl() {
    return hostUrl;
  }

  public String getUserName() {
    return userName;
  }

  public String getUserPass() {
    return userPass;
  }

  public String getMysqlDbNAme() {
    return mysqlDbNAme;
  }


  public String getMySqlConnectString() {
    return mySqlConnectString;
  }

  public String[] addUserNameAndPasswordToArgs(String[] extraArgs) {
    int extraLength = isSet(getUserPass()) ? 4 : 2;
    String[] moreArgs = new String[extraArgs.length + extraLength];
    int i = 0;
    for (i = 0; i < extraArgs.length; i++) {
      moreArgs[i] = extraArgs[i];
    }

    // Add username argument for mysql.
    moreArgs[i++] = "--username";
    moreArgs[i++] = getUserName();
    if (isSet(userPass)) {
      moreArgs[i++] = "--password";
      moreArgs[i++] = getUserPass();
    }
    return moreArgs;
  }

  public static String getCurrentUser() {
    // First, check the $USER environment variable.
    String envUser = System.getenv("USER");
    if (null != envUser) {
      return envUser;
    }
    // Try `whoami`
    String[] whoamiArgs = new String[1];
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

  public void addPasswordIfIsSet(ArrayList<String> args) {
    if (isSet(userPass)) {
      args.add("--password");
      args.add(getUserPass());
    }
  }

  private boolean isSet(String userPass) {
    return !StringUtils.isBlank(userPass);
  }

  public void addPasswordIfIsSet(SqoopOptions opts) {
    if (isSet(userPass)) {
      opts.setPassword(getUserPass());
    }
  }

}
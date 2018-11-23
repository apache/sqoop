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

package org.apache.sqoop.manager.mysql;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

  private static String getCurrentUser() {
    // First, check the $USER environment variable.
    String envUser = System.getenv("USER");
    if (null != envUser) {
      return envUser;
    }
    // Fall back to user.name system property
    envUser = System.getProperty("user.name");
    if (null != envUser) {
      return envUser;
    }
    throw new RuntimeException("MySQL username not set and unable to get system user. Please set it"
        + " with '-Dsqoop.test.mysql.username=...' or USER environment variable!");
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

  public void dropTableIfExists(String table, ConnManager manager) throws SQLException {
    Connection conn = manager.getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "DROP TABLE IF EXISTS " + table,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }
}
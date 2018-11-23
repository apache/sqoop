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

package org.apache.sqoop.manager.cubrid;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utilities for Cubrid-based tests.
 *
 * Since this requires a CUBRID installation on your local machine to use, this
 * class is named in such a way that Hadoop's default QA process does not run
 * it. You need to run this manually with -Dtestcase=CubridTestUtils
 *
 * You need to put CUBRID's Connector/J JDBC driver library into a location
 * where Hadoop will be able to access it (since this library cannot be checked
 * into Apache's tree for licensing reasons).
 *
 * To set up your test environment:
 *   Install Cubrid 9.2.2
 *   ref:http://www.cubrid.org/wiki_tutorials/entry/installing-cubrid-on-linux-using-shell-and-rpm
 *   Create a database SQOOPCUBRIDTEST
 *   $cubrid createdb SQOOPCUBRIDTEST en_us.utf8
 *   Start cubrid and database
 *   $cubrid service start
 *   $cubrid server start SQOOPCUBRIDTEST
 *   Create a login SQOOPUSER with password PASSWORD and grant all
 *   $csql -u dba SQOOPCUBRIDTEST
 *   csql>CREATE USER SQOOPUSER password 'PASSWORD';
 */
public class CubridTestUtils {

  public static final Log LOG = LogFactory.getLog(CubridTestUtils.class
      .getName());

  public static final String HOST_URL = System
      .getProperty("sqoop.test.cubrid.connectstring.host_url",
      "jdbc:cubrid:localhost:30000");

  static final String TEST_DATABASE = System
      .getProperty("sqoop.test.cubrid.connectstring.database",
      "SQOOPCUBRIDTEST");
  static final String TEST_USER = System
      .getProperty("sqoop.test.cubrid.connectstring.username",
      "SQOOPUSER");
  static final String TEST_PASS = System
      .getProperty("sqoop.test.cubrid.connectstring.password",
      "PASSWORD");
  static final String TABLE_NAME = "EMPLOYEES_CUBRID";
  static final String NULL_TABLE_NAME = "NULL_EMPLOYEES_CUBRID";
  static final String CONNECT_STRING = HOST_URL + ":"
    + TEST_DATABASE + ":::";

  public static String getCurrentUser() {
    return TEST_USER;
  }

  public static String getPassword() {
    return TEST_PASS;
  }

  public static String getConnectString() {
    return CONNECT_STRING;
  }

  public static String getTableName() {
    return TABLE_NAME;
  }
}

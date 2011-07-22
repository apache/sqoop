/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Helper methods for Oracle testing.
 */
public final class OracleUtils {

  public static final Log LOG = LogFactory.getLog(OracleUtils.class.getName());

  // Express edition hardcoded name.
  public static final String ORACLE_DATABASE_NAME = "xe";

  public static final String CONNECT_STRING =
      "jdbc:oracle:thin:@//localhost/" + ORACLE_DATABASE_NAME;
  public static final String ORACLE_USER_NAME = "SQOOPTEST";
  public static final String ORACLE_USER_PASS = "12345";

  private OracleUtils() { }

  public static void setOracleAuth(SqoopOptions options) {
    options.setUsername(ORACLE_USER_NAME);
    options.setPassword(ORACLE_USER_PASS);
  }

  /**
   * Drop a table if it exists.
   */
  public static void dropTable(String tableName, ConnManager manager)
      throws SQLException {
    Connection connection = null;
    Statement st = null;

    try {
      connection = manager.getConnection();
      connection.setAutoCommit(false);
      st = connection.createStatement();

      // create the database table and populate it with data. 
      st.executeUpdate(getDropTableStatement(tableName));

      connection.commit();
    } finally {
      try {
        if (null != st) {
          st.close();
        }
      } catch (SQLException sqlE) {
        LOG.warn("Got SQLException when closing connection: " + sqlE);
      }
    }
  }

  public static String getDropTableStatement(String tableName) {
    return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tableName + "'; "
        + "exception when others then null; end;";
  }
}

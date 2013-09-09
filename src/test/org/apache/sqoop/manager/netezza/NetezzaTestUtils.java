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

package org.apache.sqoop.manager.netezza;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.TestExport.ColumnGenerator;

/**
 * Utilities for Netezza tests.
 */
public final class NetezzaTestUtils {

  public static final Log LOG = LogFactory.getLog(
    NetezzaTestUtils.class.getName());

  public static final String NZ_HOST = System.getProperty(
    "sqoop.test.netezza.host", "nz-host");
  public static final String NZ_PORT = System.getProperty(
    "sqoop.test.netezza.port", "5480");
  public static final String NZ_JDBC_URL_PREFIX = "jdbc:netezza:";

  public static final String NZ_DB_USER = System.getProperty(
    "sqoop.test.netezza.username", "ADMIN");

  public static final String NZ_DB_PASSWORD = System.getProperty(
    "sqoop.test.netezza.password", "password");

  public static final String NZ_DB_NAME = System.getProperty(
    "sqoop.test.netezza.db.name", "SQOOP");
  public static final String TABLE_NAME = System.getProperty(
    "sqoop.test.netezza.table.name", "EMPNZ");

  private NetezzaTestUtils() {
  }

  /** @return the current username. */
  public static String getNZUser() {
    // First, check the $NZ_USER environment variable.
    String nzUser = System.getenv("NZ_USER");
    if (nzUser == null) {
      // Else return what is in the NZ Properties
      nzUser = NZ_DB_USER;
    }
    return nzUser;
  }

  public static String getNZPassword() {
    String nzPass = System.getenv("NZ_PASSWORD");
    if (nzPass == null) {
      nzPass = NZ_DB_PASSWORD;
    }
    return nzPass;
  }

  public static String getNZConnectString() {
    String nzHost = System.getenv("NZ_HOST");
    if (nzHost == null) {
      nzHost = NZ_HOST;
    }

    String nzPort = System.getenv("NZ_PORT");
    if (nzPort == null) {
      nzPort = NZ_PORT;
    }
    String nzDB = System.getenv("NZ_DB_NAME");
    if (nzDB == null) {
      nzDB = NZ_DB_NAME;
    }

    StringBuilder url = new StringBuilder(NZ_JDBC_URL_PREFIX);
    url.append("//").append(nzHost).append(':').append(nzPort);
    url.append('/').append(nzDB);

    LOG.info("NZ Connect string generated : " + url.toString());
    return url.toString();
  }

  public static String getNZDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName;
  }

  public static void createTableNZ(Connection conn, String tableName,
    ColumnGenerator... extraCols)
    throws SQLException {
    String sqlStatement = getNZDropTableStatement(tableName);
    conn.rollback();
    LOG.info("Executing drop statement : " + sqlStatement);
    PreparedStatement statement = conn.prepareStatement(
      sqlStatement, ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } catch (SQLException sqle) {
      conn.rollback();
    } finally {
      statement.close();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraCols) {
      sb.append(", col").append(colNum++).append(' ').append(gen.getType());
    }
    sb.append(")");
    sqlStatement = sb.toString();
    LOG.info("Executing create statement : " + sqlStatement);
    statement = conn.prepareStatement(sqlStatement,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }
}

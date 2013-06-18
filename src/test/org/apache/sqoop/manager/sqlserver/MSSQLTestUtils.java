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
package org.apache.sqoop.manager.sqlserver;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
* Test utilities for SQL Server manual tests.
*/
public class MSSQLTestUtils {

  public static final Log LOG = LogFactory.getLog(
      MSSQLTestUtils.class.getName());

  static final String DATABASE_USER = System.getProperty(
          "ms.sqlserver.username", "SQOOPUSER");
  static final String DATABASE_PASSWORD = System.getProperty(
          "ms.sqlserver.password", "PASSWORD");
  public static final String HOST_URL = System.getProperty(
          "sqoop.test.sqlserver.connectstring.host_url",
          "jdbc:sqlserver://sqlserverhost:1433");

  public static final String CREATE_TALBE_LINEITEM
    = "CREATE TABLE TPCH1M_LINEITEM"
    + "( [L_ORDERKEY] [int] NULL, [L_PARTKEY] "
    + "[int] NULL, [L_SUPPKEY] [int] NULL, [L_LINENUMBER] [int] NULL, "
    + "[L_QUANTITY] [int] NULL, [L_EXTENDEDPRICE] [decimal](15, 2) NULL, "
    + "[L_DISCOUNT] [decimal](15, 2) NULL, [L_TAX] [decimal](15, 2) NULL,"
    + " [L_RETURNFLAG] [varchar](max) NULL, [L_LINESTATUS] [varchar](max)"
    + " NULL, [L_SHIPDATE] [varchar](max) NULL, [L_COMMITDATE] [varchar](max)"
    + " NULL, [L_RECEIPTDATE] [varchar](max) NULL, [L_SHIPINSTRUCT] [varchar]"
    + "(max) NULL, [L_SHIPMODE] [varchar](max) NULL, [L_COMMENT] [varchar]"
    + "(max) NULL) ";

  private Connection conn = null;

  private Connection getConnection() {

    if (conn == null) {

      try {
        Connection con = DriverManager.getConnection(HOST_URL,
            DATABASE_USER, DATABASE_PASSWORD);
        conn = con;
        return con;
      } catch (SQLException e) {
        LOG.error("Get SQLException during setting up connection: " + StringUtils.stringifyException(e));
        return null;
      }
    }

    return conn;
  }

  public void createTableFromSQL(String sql) throws SQLException {
    Connection dbcon = this.getConnection();

    System.out.println("SQL : " + sql);
    this.dropTableIfExists("TPCH1M_LINEITEM");

    try {
      Statement st = dbcon.createStatement();
      int res = st.executeUpdate(sql);
      System.out.println("Result : " + res);

    } catch (SQLException e) {
      LOG.error("Got SQLException during creating table: " + StringUtils.stringifyException(e));
    }

  }

  public void populateLineItem() {
    String sql = "insert into tpch1m_lineitem values (1,2,3,4,5,6,7,8,'AB',"
        + "'CD','abcd','efgh','hijk','dothis','likethis','nocomments')";
    String sql2 = "insert into tpch1m_lineitem values (2,3,4,5,6,7,8,9,'AB'"
        + ",'CD','abcd','efgh','hijk','dothis','likethis','nocomments')";
    String sql3 = "insert into tpch1m_lineitem values (3,4,5,6,7,8,9,10,'AB',"
        + "'CD','abcd','efgh','hijk','dothis','likethis','nocomments')";
    String sql4 = "insert into tpch1m_lineitem values (4,5,6,7,8,9,10,11,'AB'"
        + ",'CD','abcd','efgh','hijk','dothis','likethis','nocomments')";
    Connection dbcon = this.getConnection();
    Statement st;
    try {
      st = dbcon.createStatement();
      st.addBatch(sql);
      st.addBatch(sql2);
      st.addBatch(sql3);
      st.addBatch(sql4);
      int[] res = st.executeBatch();

      System.out.println(res);
    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
    }

  }

  public void metadataStuff(String table) {
    Connection dbcon = this.getConnection();
    String sql = "select top 1 * from " + table;

    Statement st;
    try {

      st = dbcon.createStatement();
      ResultSet rs = st.executeQuery(sql);
      ResultSetMetaData rsmd = rs.getMetaData();

      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        System.out.println(rsmd.getColumnName(i) + "\t"
            + rsmd.getColumnClassName(i) + "\t"
            + rsmd.getColumnType(i) + "\t"
            + rsmd.getColumnTypeName(i) + "\n");
      }

    } catch (SQLException e) {
      LOG.error(StringUtils.stringifyException(e));
    }

  }

  public static String getDBUserName() {
    return DATABASE_USER;
  }

  public static String getDBPassWord() {
    return DATABASE_PASSWORD;
  }

  public void dropTableIfExists(String table) throws SQLException {
    conn = getConnection();
    System.out.println("Dropping table : " + table);
    String sqlStmt = "IF OBJECT_ID('" + table
        + "') IS NOT NULL  DROP TABLE " + table;
    PreparedStatement statement = conn.prepareStatement(sqlStmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
  }

  public static String[] getColumns() {
    return new String[] { "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY",
        "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT",
        "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE",
        "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT",
        "L_SHIPMODE", "L_COMMENT", };
  }
}

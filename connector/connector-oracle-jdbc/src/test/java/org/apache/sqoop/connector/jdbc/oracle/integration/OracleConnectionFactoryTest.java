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

package org.apache.sqoop.connector.jdbc.oracle.integration;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnectorConstants;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test OracleConnectionFactory class including initialization statements.
 */
public class OracleConnectionFactoryTest extends OracleTestCase {

  private static final String TEST_TABLE_NAME = "sqoop_conn_test";

  private static final String SQL_TABLE =
      "WITH sqltable AS "
    + "    ( "
    + "       SELECT executions, rows_processed, fetches, "
    + "              ROUND (rows_processed / executions, 2) AS rows_per_exec, "
    + "              ROUND (rows_processed / fetches, 2) AS rows_per_fetch, "
    + "              ROUND (LEAST (  ROUND (rows_processed / fetches, 2) "
    + "                            / LEAST (rows_processed / executions, 10), "
    + "                            1 "
    + "                           ), "
    + "                     2 "
    + "                    ) batch_efficiency, "
    + "              sql_text, u.username parsing_schema_name, buffer_gets, "
    + "              disk_reads, cpu_time/1000 cpu_time, elapsed_time/1000"
    + "               elapsed_time, hash_value sql_id, child_number "
    + "         FROM v$sql s join all_users u on (u.user_id=s.parsing_user_id) "
    + "        WHERE fetches > 0 AND executions > 0 AND rows_processed > 0 "
    + "          AND parsing_schema_id <> 0 AND sql_text like "
    + "                                                 'select%dba_objects' )"
    + "SELECT   sql_id, child_number, array_wastage, "
    + "         rows_processed, fetches, rows_per_exec, "
    + "        rows_per_fetch, parsing_schema_name, buffer_gets, disk_reads, "
    + "        cpu_time, elapsed_time, sql_text,executions "
    + "   FROM (SELECT sql_id, "
    + "                child_number, "
    + "                rows_processed * (1 - batch_efficiency) array_wastage, "
    + "                rows_processed, " + "                fetches, "
    + "                rows_per_exec, "
    + "                rows_per_fetch, " + "                sql_text, "
    + "                parsing_schema_name, "
    + "                buffer_gets, " + "                disk_reads, "
    + "                cpu_time, " + "                elapsed_time, "
    + "                executions " + "           FROM sqltable) ";

  @Test
  public void testSetJdbcFetchSize() {
    setAndCheckJdbcFetchSize(45);
    setAndCheckJdbcFetchSize(2000);
  }

  private void setAndCheckJdbcFetchSize(int jdbcFetchSize) {

    try {
      Connection conn = getConnection();

      String uniqueJunk =
          (new SimpleDateFormat("yyyyMMddHHmmsszzz")).format(new Date())
              + jdbcFetchSize;

      OracleQueries.setJdbcFetchSize(conn, Integer.valueOf(jdbcFetchSize));

      String uniqueSql =
          String.format("select /*%s*/ * from dba_objects", uniqueJunk);
      // Usually dba_objects will have a lot of rows
      ResultSet resultSet1 = conn.createStatement().executeQuery(uniqueSql);
      while (resultSet1.next()) {
        // Nothing to do
        continue;
      }

      ResultSet resultSet2 =
          conn.createStatement().executeQuery(SQL_TABLE);
      boolean sqlFound = false;
      double rowsPerFetch = 0;
      while (resultSet2.next()) {
        String sqlText = resultSet2.getString("SQL_TEXT");
        if (sqlText.contains(uniqueJunk)) {
          sqlFound = true;
          rowsPerFetch = resultSet2.getDouble("ROWS_PER_FETCH");
          break;
        }
      }

      if (!sqlFound) {
        Assert
            .fail("Unable to find the performance metrics for the SQL "
                + "statement being used to check the JDBC fetch size.");
      }

      if (rowsPerFetch < jdbcFetchSize * 0.95
          || rowsPerFetch > jdbcFetchSize * 1.05) {
        Assert
            .fail(String
                .format(
                    "The measured JDBC fetch size is not within 5%% of what we "
                    + "expected. Expected=%s rows/fetch, actual=%s rows/fetch",
                    jdbcFetchSize, rowsPerFetch));
      }

    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testCreateOracleJdbcConnectionBadUserName() {

    try {
      OracleConnectionFactory.createOracleJdbcConnection(
          OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
          provider.getConnectionUrl(),
          provider.getConnectionUsername() + "_INVALID",
          provider.getConnectionPassword());

      Assert
          .fail("OracleConnectionFactory should have thrown an exception in "
              + "response to a rubbish user name.");

    } catch (SQLException ex) {
      Assert.assertEquals(ex.getErrorCode(), 1017); // <- ORA-01017 invalid
                                             // username/password; logon denied.
    }
  }

  @Test
  public void testCreateOracleJdbcConnectionBadPassword() {

    try {
      OracleConnectionFactory.createOracleJdbcConnection(
          OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
          provider.getConnectionUrl(),
          provider.getConnectionUsername(),
          "a" + provider.getConnectionPassword());

      Assert
          .fail("OracleConnectionFactory should have thrown an exception in "
              + "response to a rubbish password.");

    } catch (SQLException ex) {
      Assert.assertEquals(ex.getErrorCode(), 1017); // <- ORA-01017 invalid
                                             // username/password; logon denied.
    }
  }

  @Test
  public void testCreateOracleJdbcConnectionOk() {

    try {
      Connection conn = getConnection();

      Assert.assertEquals(conn.isValid(15), true,
          "The connection to the Oracle database does not appear to be valid.");

      ResultSet resultSet =
          conn.createStatement().executeQuery(
              "select instance_name from v$instance");
      if (!resultSet.next() || resultSet.getString(1).isEmpty()) {
        Assert.fail("Got blank instance name from v$instance");
      }
    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testExecuteOraOopSessionInitializationStatements() {

    Logger log = Logger.getLogger(OracleConnectionFactory.class);
    StringWriter stringWriter = new StringWriter();
    Layout layout = new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n");
    WriterAppender writerAppender = new WriterAppender(layout, stringWriter);
    log.addAppender(writerAppender);

    // Check that the default session-initialization statements are reflected in
    // the log...
    stringWriter.getBuffer().setLength(0);
    checkExecuteOraOopSessionInitializationStatements(null);
    checkLogContainsText(stringWriter.toString(),
        "Initializing Oracle session with SQL : alter session disable "
        + "parallel query");
    checkLogContainsText(
        stringWriter.toString(),
        "Initializing Oracle session with SQL : alter session set "
        + "\"_serial_direct_read\"=true");

    // Check that the absence of session-initialization statements is reflected
    // in the log...
    List<String> statements = new ArrayList<String>();
    statements.add(" ");
    stringWriter.getBuffer().setLength(0);
    checkExecuteOraOopSessionInitializationStatements(statements);
    checkLogContainsText(stringWriter.toString(),
        "No Oracle 'session initialization' statements were found to execute");

    // This should throw an exception, as Oracle won't know what to do with
    // this...
    statements.clear();
    statements.add("loremipsum");
    stringWriter.getBuffer().setLength(0);
    checkExecuteOraOopSessionInitializationStatements(statements);
    checkLogContainsText(stringWriter.toString(), "loremipsum");
    checkLogContainsText(stringWriter.toString(),
        "ORA-00900: invalid SQL statement");

    Connection conn = getConnection();
    try {

      // Try a session-initialization statement that creates a table...
      statements.clear();
      statements.add("create table " + TEST_TABLE_NAME + " (col1 varchar2(1))");
      dropTable(conn, TEST_TABLE_NAME);
      checkExecuteOraOopSessionInitializationStatements(statements);
      if (!doesTableExist(conn, TEST_TABLE_NAME)) {
        Assert.fail("The session-initialization statement to create the table "
            + TEST_TABLE_NAME + " did not work.");
      }

      // Try a sequence of a few statements...
      statements.clear();
      statements.add("create table " + TEST_TABLE_NAME + " (col1 number)");
      statements.add("insert into " + TEST_TABLE_NAME + " values (1) ");
      statements.add("--update " + TEST_TABLE_NAME + " set col1 = col1 + 1");
      statements.add("update " + TEST_TABLE_NAME + " set col1 = col1 + 1");
      statements.add("commit");
      dropTable(conn, TEST_TABLE_NAME);
      checkExecuteOraOopSessionInitializationStatements(statements);

      ResultSet resultSet =
          conn.createStatement().executeQuery(
              "select col1 from " + TEST_TABLE_NAME);
      resultSet.next();
      int actualValue = resultSet.getInt("col1");
      if (actualValue != 2) {
        Assert.fail("The table " + TEST_TABLE_NAME
            + " does not contain the data we expected.");
      }

      dropTable(conn, TEST_TABLE_NAME);

    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
    log.removeAppender(writerAppender);
  }

  private void dropTable(Connection conn, String tableName) {

    try {
      conn.createStatement().executeQuery("drop table " + tableName);

      if (doesTableExist(conn, tableName)) {
        Assert.fail("Unable to drop the table " + tableName);
      }
    } catch (SQLException ex) {
      if (ex.getErrorCode() != 942) { // <- Table or view does not exist
        Assert.fail(ex.getMessage());
      }
    }
  }

  private boolean doesTableExist(Connection conn, String tableName) {

    boolean result = false;
    try {
      List<OracleTable> tables = OracleQueries.getTables(conn);

      for (int idx = 0; idx < tables.size(); idx++) {
        if (tables.get(idx).getName().equalsIgnoreCase(tableName)) {
          result = true;
          break;
        }
      }
    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }
    return result;
  }

  private void checkLogContainsText(String log, String text) {

    if (!log.toLowerCase().contains(text.toLowerCase())) {
      Assert.fail(
          "The LOG does not contain the following text (when it should):\n\t"
              + text);
    }
  }

  private void checkExecuteOraOopSessionInitializationStatements(
      List<String> statements) {

    Connection conn = getConnection();

    OracleConnectionFactory.executeOraOopSessionInitializationStatements(
        conn, statements);
  }

  @Test
  public void testSetSessionClientInfo() {

    Connection conn = getConnection();

    ConnectionConfig connectionConfig = new ConnectionConfig();

    String moduleName = OracleJdbcConnectorConstants.ORACLE_SESSION_MODULE_NAME;
    String actionName =
        (new SimpleDateFormat("yyyyMMddHHmmsszzz")).format(new Date());

    connectionConfig.actionName = actionName;

    try {
      PreparedStatement statement =
          conn.prepareStatement("select process, module, action "
              + "from v$session " + "where module = ? and action = ?");
      statement.setString(1, moduleName);
      statement.setString(2, actionName);

      // Check no session have this action name - because we haven't applied to
      // our session yet...
      ResultSet resultSet = statement.executeQuery();
      if (resultSet.next()) {
        Assert
            .fail("There should be no Oracle sessions with an action name of "
                + actionName);
      }

      // Apply this action name to our session...
      OracleConnectionFactory.setSessionClientInfo(conn, connectionConfig);

      // Now check there is a session with our action name...
      int sessionFoundCount = 0;
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        sessionFoundCount++;
      }

      if (sessionFoundCount < 1) {
        Assert
            .fail("Unable to locate an Oracle session with the expected module "
                + "and action.");
      }

      if (sessionFoundCount > 1) {
        Assert
            .fail("Multiple sessions were found with the expected module and "
                + "action - we only expected to find one.");
      }
    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }

  }

  private Connection getConnection() {

    try {
      return OracleConnectionFactory.createOracleJdbcConnection(
          OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
          provider.getConnectionUrl(),
          provider.getConnectionUsername(), provider.getConnectionPassword());
    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }
    return null;
  }

}

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

package org.apache.sqoop.manager.oracle;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.sqoop.manager.OracleUtils;

/**
 * Test OracleConnectionFactory class including initialization statements.
 */
public class OracleConnectionFactoryTest extends OraOopTestCase {

  /**
   * This is just to expose methods in OracleConnectionFactory.
   */
  public class Exposer extends OracleConnectionFactory {

  }

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

      org.apache.hadoop.conf.Configuration conf = new Configuration();
      conf.setInt(OraOopConstants.ORACLE_ROW_FETCH_SIZE, jdbcFetchSize);

      // Prevent setJdbcFetchSize() from logging information about the
      // fetch-size
      // changing. Otherwise, the junit output will be polluted with messages
      // about
      // things that aren't actually a problem...
      boolean logIsBeingCached =
          OracleConnectionFactory.LOG.getCacheLogEntries();
      OracleConnectionFactory.LOG.setCacheLogEntries(true);

      OraOopOracleQueries.setJdbcFetchSize(conn, conf);

      OracleConnectionFactory.LOG.setCacheLogEntries(logIsBeingCached);

      String uniqueSql =
          String.format("select /*%s*/ * from dba_objects", uniqueJunk);
      // Usually dba_objects will have a lot of rows
      ResultSet resultSet1 = conn.createStatement().executeQuery(uniqueSql);
      while (resultSet1.next()) {
        // Nothing to do
        continue;
      }

      ResultSet resultSet2 =
          conn.createStatement().executeQuery(OraOopTestConstants.SQL_TABLE);
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

      // Prevent createOracleJdbcConnection() from logging a problem with the
      // bad username we're about to use. Otherwise, the junit output will be
      // polluted with messages about things that aren't actually a problem...
      boolean logIsBeingCached =
          OracleConnectionFactory.LOG.getCacheLogEntries();
      OracleConnectionFactory.LOG.setCacheLogEntries(true);

      OracleConnectionFactory.createOracleJdbcConnection(
          OraOopConstants.ORACLE_JDBC_DRIVER_CLASS, OracleUtils.CONNECT_STRING,
          OracleUtils.ORACLE_INVALID_USER_NAME, OracleUtils.ORACLE_USER_PASS);

      OracleConnectionFactory.LOG.setCacheLogEntries(logIsBeingCached);

      Assert
          .fail("OracleConnectionFactory should have thrown an exception in "
              + "response to a rubbish user name.");

    } catch (SQLException ex) {
      assertEquals(ex.getErrorCode(), 1017); // <- ORA-01017 invalid
                                             // username/password; logon denied.
    }
  }

  @Test
  public void testCreateOracleJdbcConnectionBadPassword() {

    try {
      // Prevent createOracleJdbcConnection() from logging a problem with the
      // bad username we're about to use. Otherwise, the junit output will be
      // polluted with messages about things that aren't actually a problem...
      boolean logIsBeingCached =
          OracleConnectionFactory.LOG.getCacheLogEntries();
      OracleConnectionFactory.LOG.setCacheLogEntries(true);

      OracleConnectionFactory.createOracleJdbcConnection(
          OraOopConstants.ORACLE_JDBC_DRIVER_CLASS, OracleUtils.CONNECT_STRING,
          OracleUtils.ORACLE_USER_NAME, "a" + OracleUtils.ORACLE_USER_PASS);

      OracleConnectionFactory.LOG.setCacheLogEntries(logIsBeingCached);

      Assert
          .fail("OracleConnectionFactory should have thrown an exception in "
              + "response to a rubbish password.");

    } catch (SQLException ex) {
      assertEquals(ex.getErrorCode(), 1017); // <- ORA-01017 invalid
                                             // username/password; logon denied.
    }
  }

  @Test
  public void testCreateOracleJdbcConnectionOk() {

    try {
      Connection conn = getConnection();

      assertEquals(
          "The connection to the Oracle database does not appear to be valid.",
          true, conn.isValid(15));

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

    // Exposer.LOG = null;
    // protected static final Log LOG =
    // LogFactory.getLog(OracleConnectionFactory.class.getName());

    OraOopLogFactory.OraOopLog2 oraoopLog = Exposer.LOG;

    oraoopLog.setCacheLogEntries(true);

    // Check that the default session-initialization statements are reflected in
    // the log...
    oraoopLog.clearCache();
    checkExecuteOraOopSessionInitializationStatements(null);
    checkLogContainsText(oraoopLog,
        "Initializing Oracle session with SQL : alter session disable "
        + "parallel query");
    checkLogContainsText(
        oraoopLog,
        "Initializing Oracle session with SQL : alter session set "
        + "\"_serial_direct_read\"=true");

    // Check that the absence of session-initialization statements is reflected
    // in the log...
    oraoopLog.clearCache();
    checkExecuteOraOopSessionInitializationStatements("");
    checkLogContainsText(oraoopLog,
        "No Oracle 'session initialization' statements were found to execute");

    // This should do nothing (i.e. not throw an exception)...
    checkExecuteOraOopSessionInitializationStatements(";");

    // This should throw an exception, as Oracle won't know what to do with
    // this...
    oraoopLog.clearCache();
    checkExecuteOraOopSessionInitializationStatements("loremipsum");
    checkLogContainsText(oraoopLog, "loremipsum");
    checkLogContainsText(oraoopLog, "ORA-00900: invalid SQL statement");

    Connection conn = getConnection();
    try {

      // Try a session-initialization statement that creates a table...
      dropTable(conn, OracleUtils.SYSTEMTEST_TABLE_NAME);
      checkExecuteOraOopSessionInitializationStatements("create table "
          + OracleUtils.SYSTEMTEST_TABLE_NAME + " (col1 varchar2(1))");
      if (!doesTableExist(conn, OracleUtils.SYSTEMTEST_TABLE_NAME)) {
        Assert.fail("The session-initialization statement to create the table "
            + OracleUtils.SYSTEMTEST_TABLE_NAME + " did not work.");
      }

      // Try a sequence of a few statements...
      dropTable(conn, OracleUtils.SYSTEMTEST_TABLE_NAME);
      checkExecuteOraOopSessionInitializationStatements("create table "
          + OracleUtils.SYSTEMTEST_TABLE_NAME + " (col1 number);insert into "
          + OracleUtils.SYSTEMTEST_TABLE_NAME + " values (1) ; --update "
          + OracleUtils.SYSTEMTEST_TABLE_NAME + " set col1 = col1 + 1; update "
          + OracleUtils.SYSTEMTEST_TABLE_NAME
          + " set col1 = col1 + 1; commit ;;");

      ResultSet resultSet =
          conn.createStatement().executeQuery(
              "select col1 from " + OracleUtils.SYSTEMTEST_TABLE_NAME);
      resultSet.next();
      int actualValue = resultSet.getInt("col1");
      if (actualValue != 2) {
        Assert.fail("The table " + OracleUtils.SYSTEMTEST_TABLE_NAME
            + " does not contain the data we expected.");
      }

      dropTable(conn, OracleUtils.SYSTEMTEST_TABLE_NAME);

    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testParseOraOopSessionInitializationStatements() {

    List<String> statements = null;

    try {
      statements =
          OracleConnectionFactory
              .parseOraOopSessionInitializationStatements(null);
      Assert.fail("An IllegalArgumentException should have been thrown.");
    } catch (IllegalArgumentException ex) {
      // This is what we wanted.
    }

    org.apache.hadoop.conf.Configuration conf = new Configuration();

    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertTrue(statements.size() > 0);

    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS, "");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(0, statements.size());

    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS, ";");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(0, statements.size());

    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS,
        ";--;\t--");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(0, statements.size());

    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS
        , "\ta");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(1, statements.size());
    if (!statements.get(0).equalsIgnoreCase("a")) {
      Assert.fail("Expected a session initialization statement of \"a\"");
    }

    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS,
        "a;b;--c;d;");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(3, statements.size());
    if (!statements.get(0).equalsIgnoreCase("a")) {
      Assert.fail("Expected a session initialization statement of \"a\"");
    }
    if (!statements.get(1).equalsIgnoreCase("b")) {
      Assert.fail("Expected a session initialization statement of \"b\"");
    }
    if (!statements.get(2).equalsIgnoreCase("d")) {
      Assert.fail("Expected a session initialization statement of \"d\"");
    }

    // Expressions without default values...
    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS,
        "set a={expr1};b={expr2}/{expr3};");
    conf.set("expr1", "1");
    conf.set("expr2", "2");
    conf.set("expr3", "3");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(2, statements.size());
    String actual = statements.get(0);
    String expected = "set a=1";
    if (!actual.equalsIgnoreCase(expected)) {
      Assert.fail(String.format(
        "Expected a session initialization statement of \"%s\", but got \"%s\"."
                  , expected, actual));
    }
    actual = statements.get(1);
    expected = "b=2/3";
    if (!actual.equalsIgnoreCase(expected)) {
      Assert.fail(String.format(
        "Expected a session initialization statement of \"%s\", but got \"%s\"."
                  , expected, actual));
    }

    // Expressions with default values...
    conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS,
        "set c={expr3|66};d={expr4|15}/{expr5|90};");
    conf.set("expr3", "20");
    // conf.set("expr4", "21");
    // conf.set("expr5", "23");
    statements =
        OracleConnectionFactory
            .parseOraOopSessionInitializationStatements(conf);
    Assert.assertEquals(2, statements.size());
    actual = statements.get(0);
    expected = "set c=20";
    if (!actual.equalsIgnoreCase(expected)) {
      Assert.fail(String.format(
        "Expected a session initialization statement of \"%s\", but got \"%s\"."
                  , expected, actual));
    }
    actual = statements.get(1);
    expected = "d=15/90";
    if (!actual.equalsIgnoreCase(expected)) {
      Assert.fail(String.format(
        "Expected a session initialization statement of \"%s\", but got \"%s\"."
                  , expected, actual));
    }

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
      List<OracleTable> tables = OraOopOracleQueries.getTables(conn);

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

  private void checkLogContainsText(OraOopLogFactory.OraOopLog2 oraoopLog,
      String text) {

    if (!oraoopLog.getLogEntries().toLowerCase().contains(text.toLowerCase())) {
      Assert.fail(
          "The LOG does not contain the following text (when it should):\n\t"
              + text);
    }
  }

  private void checkExecuteOraOopSessionInitializationStatements(
      String statements) {

    Connection conn = getConnection();

    org.apache.hadoop.conf.Configuration conf = new Configuration();
    if (statements != null) {
      conf.set(OraOopConstants.ORAOOP_SESSION_INITIALIZATION_STATEMENTS,
          statements);
    }

    Exposer.executeOraOopSessionInitializationStatements(conn, conf);
  }

  @Test
  public void testSetSessionClientInfo() {

    Connection conn = getConnection();

    org.apache.hadoop.conf.Configuration conf = new Configuration();

    String moduleName = OraOopConstants.ORACLE_SESSION_MODULE_NAME;
    String actionName =
        (new SimpleDateFormat("yyyyMMddHHmmsszzz")).format(new Date());

    conf.set(OraOopConstants.ORACLE_SESSION_ACTION_NAME, actionName);

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
      OracleConnectionFactory.setSessionClientInfo(conn, conf);

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
          OraOopConstants.ORACLE_JDBC_DRIVER_CLASS, OracleUtils.CONNECT_STRING,
          OracleUtils.ORACLE_USER_NAME, OracleUtils.ORACLE_USER_PASS);
    } catch (SQLException ex) {
      Assert.fail(ex.getMessage());
    }
    return null;
  }

}

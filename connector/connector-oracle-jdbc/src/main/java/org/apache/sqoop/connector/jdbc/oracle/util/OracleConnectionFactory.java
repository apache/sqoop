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

package org.apache.sqoop.connector.jdbc.oracle.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnectorConstants;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;

/**
 * Create and initialize connections to Oracle RDBMS.
 */
public class OracleConnectionFactory {

  protected OracleConnectionFactory() {
  }

  private static final Logger LOG =
      Logger.getLogger(OracleConnectionFactory.class);

  public static Connection makeConnection(ConnectionConfig config) throws SQLException {

    String connectStr = config.connectionString;
    String username = config.username;
    String password = config.password;
    Properties additionalProps = new Properties();
    if(config.jdbcProperties != null) {
      additionalProps.putAll(config.jdbcProperties);
    }

    Connection connection =
        OracleConnectionFactory.createOracleJdbcConnection(
            OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
            connectStr, username, password, additionalProps);
    //TODO: This is from the other Oracle Manager
//    if (username == null) {
//      username = OracleManager.getSessionUser(connection);
//    }
    OracleUtilities.setCurrentSessionUser(username);
    return connection;
  }

  public static Connection createOracleJdbcConnection(
      String jdbcDriverClassName, String jdbcUrl, String username,
      String password) throws SQLException {
    Properties props = null;
    return createOracleJdbcConnection(jdbcDriverClassName, jdbcUrl, username,
        password, props);
  }

  public static Connection createOracleJdbcConnection(
      String jdbcDriverClassName, String jdbcUrl, String username,
      String password, Properties additionalProps) throws SQLException {

    loadJdbcDriver(jdbcDriverClassName);
    Connection connection =
        createConnection(jdbcUrl, username, password, additionalProps);

    // Only OraOopDBRecordReader will call initializeOracleConnection(), as
    // we only need to initialize the session(s) prior to the mapper starting
    // it's job.
    // i.e. We don't need to initialize the sessions in order to get the
    // table's data-files etc.

    // initializeOracleConnection(connection, conf);

    return connection;
  }

  private static void loadJdbcDriver(String jdbcDriverClassName) {

    try {
      Class.forName(jdbcDriverClassName);
    } catch (ClassNotFoundException ex) {
      String errorMsg =
          "Unable to load the jdbc driver class : " + jdbcDriverClassName;
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  private static Connection createConnection(String jdbcUrl, String username,
      String password, Properties additionalProps) throws SQLException {

    Properties props = new Properties();
    if (username != null) {
      props.put("user", username);
    }

    if (password != null) {
      props.put("password", password);
    }

    if (additionalProps != null && additionalProps.size() > 0) {
      props.putAll(additionalProps);
    }

    OracleUtilities.checkJavaSecurityEgd();

    try {
      Connection result = DriverManager.getConnection(jdbcUrl, props);
      result.setAutoCommit(false);
      return result;
    } catch (SQLException ex) {
      String errorMsg = String.format(
        "Unable to obtain a JDBC connection to the URL \"%s\" as user \"%s\": ",
        jdbcUrl, (username != null) ? username : "[null]");
      LOG.error(errorMsg, ex);
      throw ex;
    }
  }

  public static void initializeOracleConnection(Connection connection,
      ConnectionConfig config) throws SQLException {

    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    connection.setAutoCommit(false);

    OracleQueries.setConnectionTimeZone(connection, config.timeZone);

    setSessionClientInfo(connection, config);

    OracleQueries.setJdbcFetchSize(connection, config.fetchSize);

    executeOraOopSessionInitializationStatements(connection,
        config.initializationStatements);
  }

  public static void setSessionClientInfo(Connection connection,
      ConnectionConfig config) {

    String sql = "";
    try {
      sql =
          "begin \n"
              + "  dbms_application_info.set_module(module_name => "
              + "'%s', action_name => '%s'); \n"
              + "end;";

      String oracleSessionActionName = config.actionName;

      sql =
          String.format(sql,
              OracleJdbcConnectorConstants.ORACLE_SESSION_MODULE_NAME,
              oracleSessionActionName);

      Statement statement = connection.createStatement();
      statement.execute(sql);
      LOG.info("Initializing Oracle session with SQL :\n" + sql);
    } catch (Exception ex) {
      LOG.error(String.format("An error occurred while attempting to execute "
          + "the following Oracle session-initialization statement:" + "\n%s"
          + "\nError:" + "\n%s", sql, ex.getMessage()));
    }
  }

  public static void executeOraOopSessionInitializationStatements(
      Connection connection, List<String> sessionInitializationStatements) {
    List<String> statements = sessionInitializationStatements;

    if(statements == null || statements.isEmpty()) {
      statements = OracleJdbcConnectorConstants.
          ORACLE_SESSION_INITIALIZATION_STATEMENTS_DEFAULT;
    }

    int numStatements = 0;
    for (String statement : statements) {
      String initializationStatement = statement.trim();
      if (initializationStatement != null
          && !initializationStatement.isEmpty()
          && !initializationStatement
              .startsWith(OracleJdbcConnectorConstants.Oracle.
                  ORACLE_SQL_STATEMENT_COMMENT_TOKEN)) {
        try {
          numStatements++;
          connection.createStatement().execute(statement);
          LOG.info("Initializing Oracle session with SQL : " + statement);
        } catch (Exception ex) {
          LOG.error(String.format(
              "An error occurred while attempting to execute "
                  + "the following Oracle session-initialization statement:"
                  + "\n%s" + "\nError:" + "\n%s", statement, ex.getMessage()));
        }
      }
    }
    if(numStatements==0) {
      LOG.warn("No Oracle 'session initialization' statements were found to "
          + "execute.");
    }
  }

}

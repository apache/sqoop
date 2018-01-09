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

package org.apache.sqoop.testutil;

import java.util.Arrays;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Server;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.HsqldbManager;

/**
 * Create a simple hsqldb server and schema to use for testing.
 */
public class HsqldbTestServer {
  public static final Log LOG =
    LogFactory.getLog(HsqldbTestServer.class.getName());

  // singleton server instance.
  private static Server server;

  // if -Dhsql.server.host hasn't been set to something like
  // hsql://localhost.localdomain/ a defaul in-mem DB will be used
  private static final String IN_MEM = "mem:";
  public static String getServerHost() {
    String host = System.getProperty("hsql.server.host", IN_MEM);
    if (!host.endsWith("/")) { host += "/"; }
    return host;
  }

  private static boolean inMemoryDB = IN_MEM.equals(getServerHost());

  // Database name can be altered too
  private static final String DATABASE_NAME =
      System.getProperty("hsql.database.name",  "db1");

  // hsqldb always capitalizes table and column names
  private static final String DUMMY_TABLE_NAME = "TWOINTTABLE";
  private static final String [] TWO_INT_TABLE_FIELDS = {
    "INTFIELD1",
    "INTFIELD2",
  };

  private static final String EMPLOYEE_TABLE_NAME = "EMPLOYEES";

  private static final String DB_URL = "jdbc:hsqldb:"
      + getServerHost() + DATABASE_NAME;
  private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";

  // all user-created HSQLDB tables are in the "PUBLIC" schema when connected
  // to a database.
  private static final String HSQLDB_SCHEMA_NAME = "PUBLIC";

  public static String getSchemaName() {
    return HSQLDB_SCHEMA_NAME;
  }

  public static String [] getFieldNames() {
    return Arrays.copyOf(TWO_INT_TABLE_FIELDS, TWO_INT_TABLE_FIELDS.length);
  }

  public static String getUrl() {
    return DB_URL;
  }

  public static String getTableName() {
    return DUMMY_TABLE_NAME;
  }

  public static String getDatabaseName() {
    return DATABASE_NAME;
  }

  /**
   * start the server.
   */
  public void start() {
    if (null == server) {
      LOG.info("Starting new hsqldb server; database=" + DATABASE_NAME);
      String tmpDir = System.getProperty("test.build.data", "/tmp/");
      String dbLocation = tmpDir + "/sqoop/testdb.file";
      if (inMemoryDB) {dbLocation = IN_MEM; }
      server = new Server();

      server.setDatabaseName(0, DATABASE_NAME);
      server.putPropertiesFromString("database.0=" + dbLocation
          + ";no_system_exit=true");
      server.start();
    }
  }

  public void stop() {
    if (null == server) {
      return;
    }
    server.stop();
    server = null;
  }

  public Connection getConnection() throws SQLException {
    return getConnection(null, null);
  }

  public Connection getConnection(String user, String password) throws SQLException {
    try {
      Class.forName(DRIVER_CLASS);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Could not get connection; driver class not found: "
          + DRIVER_CLASS);
      return null;
    }

    Connection connection = DriverManager.getConnection(DB_URL, user, password);
    connection.setAutoCommit(false);
    return connection;
  }

  /**
   * Returns database URL for the server instance.
   * @return String representation of DB_URL
   */
  public static String getDbUrl() {
    return DB_URL;
  }

  /**
   * Create a table.
   */
  public void createSchema() throws SQLException {

    Connection connection = null;
    Statement st = null;

    try {
      connection = getConnection();

      st = connection.createStatement();
      st.executeUpdate("DROP TABLE \"" + DUMMY_TABLE_NAME + "\" IF EXISTS");
      st.executeUpdate("CREATE TABLE \"" + DUMMY_TABLE_NAME + "\"(intField1 INT, intField2 INT)");

      connection.commit();
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }
  }

  /**
   * @return the sum of the integers in the first column of TWOINTTABLE.
   */
  public static int getFirstColSum() {
    return 1 + 3 + 5 + 7;
  }

  /**
   * Fill the table with some data.
   */
  public void populateData() throws SQLException {

    Connection connection = null;
    Statement st = null;

    try {
      connection = getConnection();

      st = connection.createStatement();
      st.executeUpdate("INSERT INTO \"" + DUMMY_TABLE_NAME + "\" VALUES(1, 8)");
      st.executeUpdate("INSERT INTO \"" + DUMMY_TABLE_NAME + "\" VALUES(3, 6)");
      st.executeUpdate("INSERT INTO \"" + DUMMY_TABLE_NAME + "\" VALUES(5, 4)");
      st.executeUpdate("INSERT INTO \"" + DUMMY_TABLE_NAME + "\" VALUES(7, 2)");

      connection.commit();
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }
  }

  public void createEmployeeDemo() throws SQLException, ClassNotFoundException {
    Class.forName(DRIVER_CLASS);

    Connection connection = null;
    Statement st = null;

    try {
      connection = getConnection();

      st = connection.createStatement();
      st.executeUpdate("DROP TABLE \"" + EMPLOYEE_TABLE_NAME + "\" IF EXISTS");
      st.executeUpdate("CREATE TABLE \"" + EMPLOYEE_TABLE_NAME + "\"(emp_id INT NOT NULL PRIMARY KEY, name VARCHAR(64))");

      st.executeUpdate("INSERT INTO \"" + EMPLOYEE_TABLE_NAME + "\" VALUES(1, 'Aaron')");
      st.executeUpdate("INSERT INTO \"" + EMPLOYEE_TABLE_NAME + "\" VALUES(2, 'Joe')");
      st.executeUpdate("INSERT INTO \"" + EMPLOYEE_TABLE_NAME + "\" VALUES(3, 'Jim')");
      st.executeUpdate("INSERT INTO \"" + EMPLOYEE_TABLE_NAME + "\" VALUES(4, 'Lisa')");

      connection.commit();
    } finally {
      if (null != st) {
        st.close();
      }

      if (null != connection) {
        connection.close();
      }
    }
  }

  /**
   * Delete any existing tables.
   */
  public void dropExistingSchema() throws SQLException {
    ConnManager mgr = getManager();
    String [] tables = mgr.listTables();
    if (null != tables) {
      Connection conn = mgr.getConnection();
      for (String table : tables) {
        Statement s = conn.createStatement();
        try {
          s.executeUpdate("DROP TABLE \"" + table + "\"");
          conn.commit();
        } finally {
          s.close();
        }
      }
    }
  }

  /**
   * Creates an hsqldb server, fills it with tables and data.
   */
  public void resetServer() throws ClassNotFoundException, SQLException {
    start();
    dropExistingSchema();
    createSchema();
    populateData();
  }

  public SqoopOptions getSqoopOptions() {
    return new SqoopOptions(HsqldbTestServer.getUrl(),
        HsqldbTestServer.getTableName());
  }

  public ConnManager getManager() {
    return new HsqldbManager(getSqoopOptions());
  }

  public void createNewUser(String username, String password) throws SQLException {
    try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("CREATE USER %s PASSWORD %s ADMIN", username, password));
    }
  }

  public void changePasswordForUser(String username, String newPassword) throws SQLException {
    try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
      statement.executeUpdate(String.format("ALTER USER %s SET PASSWORD %s", username, newPassword));
    }
  }

}

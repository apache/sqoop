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
package org.apache.sqoop.test.db;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

/**
 * Database provider for testing purpose.
 *
 * Provider contains all methods needed to bootstrap and run the tests on remote
 * databases. This is abstract implementation that is database agnostic. Each
 * supported database server have it's own concrete implementation that fills
 * the gaps in database differences.
 */
abstract public class DatabaseProvider {

  private static final Logger LOG = Logger.getLogger(DatabaseProvider.class);

  /**
   * Internal connection to the database.
   */
  private Connection connection;

  /**
   * JDBC Url to the remote database system.
   *
   * This will be passed to the Sqoop2 server during tests.
   *
   * @return String
   */
  abstract public String getConnectionUrl();

  /**
   * Connection username.
   *
   * This will be passed to the Sqoop2 server during tests.
   *
   * @return String
   */
  abstract public String getConnectionUsername();

  /**
   * Connection password.
   *
   * This will be passed to the Sqoop2 server during tests.
   *
   * @return String
   */
  abstract public String getConnectionPassword();

  /**
   * Escape column name based on specific database requirements.
   *
   * @param columnName Column name
   * @return Escaped column name
   */
  abstract public String escapeColumnName(String columnName);

  /**
   * Escape table name based on specific database requirements.
   *
   * @param tableName Table name
   * @return Escaped table name
   */
  abstract public String escapeTableName(String tableName);

  /**
   * Escape string value that can be safely used in the queries.
   *
   * @param value String value
   * @return Escaped string value
   */
  abstract public String escapeValueString(String value);

  /**
   * String constant that can be used to denote null (unknown) value.
   *
   * @return String encoding null value
   */
  public String nullConstant() {
    return "NULL";
  }

  /**
   * True if the underlying database supports custom schemes (namespaces).
   *
   * @return
   */
  public boolean isSupportingScheme() {
    return false;
  }

  public String getJdbcDriver() {
    return null;
  }

  /**
   * Start the handler.
   */
  public void start() {
    if(getJdbcDriver() != null) {
      loadClass(getJdbcDriver());
    }

    // Create connection to the database server
    try {
      setConnection(DriverManager.getConnection(getConnectionUrl(), getConnectionUsername(), getConnectionPassword()));
    } catch (SQLException e) {
      LOG.error("Can't create connection", e);
      throw new RuntimeException("Can't create connection", e);
    }
  }

  /**
   * Stop the handler.
   */
  public void stop() {
   // Close connection to the database server
   if(connection != null) {
     try {
       connection.close();
     } catch (SQLException e) {
       LOG.info("Ignored exception on closing connection", e);
     }
   }
  }

  /**
   * Return connection to the database.
   *
   * @return
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * Set connection to a new object.
   *
   * @param connection New connection object
   */
  protected void setConnection(Connection connection) {
    this.connection = connection;
  }

  /**
   * Execute DDL or DML query.
   *
   * This method will throw RuntimeException on failure.
   *
   * @param query DDL or DML query.
   */
  public void executeUpdate(String query) {
    LOG.info("Executing query: " + query);
    Statement stmt = null;

    try {
      stmt = connection.createStatement();
      stmt.executeUpdate(query);
    } catch (SQLException e) {
      LOG.error("Error in executing query", e);
      throw new RuntimeException("Error in executing query", e);
    } finally {
      try {
        if(stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        LOG.info("Cant' close statement", e);
      }
    }
  }

  /**
   * Create new table.
   *
   * @param name Table name
   * @param primaryKey Primary key column(0) or null if table should not have any
   * @param columns List of double values column name and value for example ... "id", "varchar(50)"...
   */
  public void createTable(String name, String primaryKey, String ...columns) {
    // Columns are in form of two strings - name and type
    if(columns.length == 0  || columns.length % 2 != 0) {
      throw new RuntimeException("Incorrect number of parameters.");
    }

    // Drop the table in case that it already exists
    dropTable(name);

    StringBuilder sb = new StringBuilder("CREATE TABLE ");
    sb.append(escapeTableName(name)).append("(");

    // Column list
    List<String> columnList = new LinkedList<String>();
    for(int i = 0; i < columns.length; i += 2) {
      String column = escapeColumnName(columns[i]) + " " + columns[i + 1];
      columnList.add(column);
    }
    sb.append(StringUtils.join(columnList, ", "));

    if(primaryKey != null) {
      sb.append(", PRIMARY KEY(").append(escapeColumnName(primaryKey)).append(")");
    }

    sb.append(")");

    executeUpdate(sb.toString());
  }

  /**
   * Insert new row into the table.
   *
   * @param tableName Table name
   * @param values List of objects that should be inserted
   */
  public void insertRow(String tableName, Object ...values) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(escapeTableName(tableName));
    sb.append(" VALUES (");

    List<String> valueList = new LinkedList<String>();
    for(Object value : values) {
      if(value == null) {
        valueList.add(nullConstant());
      } else if(value.getClass() == String.class) {
        valueList.add(escapeValueString((String)value));
      } else {
        valueList.add(value.toString());
      }
    }

    sb.append(StringUtils.join(valueList, ", "));
    sb.append(")");

    executeUpdate(sb.toString());
  }

  /**
   * Drop table.
   *
   * Any exceptions will be ignored.
   *
   * @param tableName
   */
  public void dropTable(String tableName) {
    StringBuilder sb = new StringBuilder("DROP TABLE ");
    sb.append(escapeTableName(tableName));

    try {
      executeUpdate(sb.toString());
    } catch(RuntimeException e) {
      LOG.info("Ignoring exception: " + e);
    }
  }

  /**
   * Load class.
   *
   * @param className Class name
   */
  public void loadClass(String className) {
    try {
      Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found: " + className, e);
    }
  }
}

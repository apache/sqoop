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
package org.apache.sqoop.common.test.db;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.test.db.types.DatabaseType;
import org.apache.sqoop.common.test.db.types.DatabaseTypeList;
import org.apache.sqoop.common.test.db.types.EmptyTypeList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
  private Connection databaseConnection;

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
   * Escape schema name based on specific database requirements.
   *
   * @param schemaName Schema name
   * @return Escaped schemaname
   */
  public String escapeSchemaName(String schemaName) {
    if (!isSupportingScheme()) {
      throw new UnsupportedOperationException("Schema is not supported in this database");
    }

    return schemaName;
  }

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

  /**
   * JDBC Driver class name.
   *
   * Fully qualified name of the driver class, so that Class.forName() or
   * similar facility can be used.
   *
   * @return
   */
  public String getJdbcDriver() {
    return null;
  }

  /**
   * Return type overview for this database.
   *
   * This method must work even in case that the provider hasn't been started.
   *
   * @return
   */
  public DatabaseTypeList getDatabaseTypes() {
    return new EmptyTypeList();
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
   if(databaseConnection != null) {
     try {
       databaseConnection.close();
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
    return databaseConnection;
  }

  /**
   * Set connection to a new object.
   *
   * @param connection New connection object
   */
  protected void setConnection(Connection connection) {
    databaseConnection = connection;
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
      stmt = databaseConnection.createStatement();
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
   * Execute given query in a new statement object and return corresponding
   * result set. Caller is responsible for closing both ResultSet and Statement
   * object!
   *
   * @param query Query to execute
   * @return Generated ResultSet
   */
  public ResultSet executeQuery(String query) {
    LOG.info("Executing query: " + query);
    Statement stmt = null;

    try {
      stmt = databaseConnection.createStatement();
      return stmt.executeQuery(query);
    } catch (SQLException e) {
      LOG.error("Error in executing query", e);
      throw new RuntimeException("Error in executing query", e);
    }
  }

  /**
   * Execute given insert query in a new statement object and return
   * generated IDs.
   *
   * @param query Query to execute
   * @return Generated ID.
   */
  public Long executeInsertQuery(String query, Object... args) {
    LOG.info("Executing query: " + query);
    ResultSet rs = null;

    try {
      PreparedStatement stmt = databaseConnection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);
      for (int i = 0; i < args.length; ++i) {
        if (args[i] instanceof String) {
          stmt.setString(i + 1, (String) args[i]);
        } else if (args[i] instanceof Long) {
          stmt.setLong(i + 1, (Long) args[i]);
        } else if (args[i] instanceof Boolean) {
          stmt.setBoolean(i + 1, (Boolean) args[i]);
        } else {
          stmt.setObject(i + 1, args[i]);
        }
      }

      stmt.execute();
      rs = stmt.getGeneratedKeys();
      if (rs.next()) {
        return rs.getLong(1);
      }
    } catch (SQLException e) {
      LOG.error("Error in executing query", e);
      throw new RuntimeException("Error in executing query", e);
    } finally {
      closeResultSetWithStatement(rs);
    }

    return -1L;
  }

  /**
   * Create new table.
   *
   * @param name Table name
   * @param primaryKey Primary key column(0) or null if table should not have any
   * @param columns List of double values column name and value for example ... "id", "varchar(50)"...
   */
  public void createTable(TableName name, String primaryKey, String ...columns) {
    // Columns are in form of two strings - name and type
    if(columns.length == 0  || columns.length % 2 != 0) {
      throw new RuntimeException("Incorrect number of parameters.");
    }

    // Drop the table in case that it already exists
    dropTable(name);

    StringBuilder sb = new StringBuilder("CREATE TABLE ");
    sb.append(getTableFragment(name)).append("(");

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
  public void insertRow(TableName tableName, Object ...values) {
    insertRow(tableName, true, values);
  }

  /**
   * Insert new row into the table.
   *
   * @param tableName Table name
   * @param escapeValues Should the values be escaped based on their type or not
   * @param values List of objects that should be inserted
   */
  public void insertRow(TableName tableName, boolean escapeValues, Object ...values) {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    sb.append(getTableFragment(tableName));
    sb.append(" VALUES (");

    List<String> valueList = new LinkedList<String>();
    for(Object value : values) {
      valueList.add(escapeValues ? convertObjectToQueryString(value) : value.toString());
    }

    sb.append(StringUtils.join(valueList, ", "));
    sb.append(")");

    executeUpdate(sb.toString());
  }

  /**
   * Return rows that match given conditions.
   *
   * @param tableName Table name
   * @param conditions Conditions in form of double values - column name and value, for example: "id", 1 or "last_update_date", null
   * @return ResultSet with given criteria
   */
  public ResultSet getRows(TableName tableName, Object []conditions) {
    return getRows(tableName, true, conditions);
  }

  /**
   * Return rows that match given conditions.
   *
   * @param tableName Table name
   * @param escapeValues Should the values be escaped based on their type or not
   * @param conditions Conditions in form of double values - column name and value, for example: "id", 1 or "last_update_date", null
   * @return ResultSet with given criteria
   */
  public ResultSet getRows(TableName tableName, boolean escapeValues, Object []conditions) {
    // Columns are in form of two strings - name and value
    if(conditions.length % 2 != 0) {
      throw new RuntimeException("Incorrect number of parameters.");
    }

    StringBuilder sb = new StringBuilder("SELECT * FROM ");
    sb.append(getTableFragment(tableName));

    List<String> conditionList = new LinkedList<String>();
    for(int i = 0; i < conditions.length; i += 2) {
      Object columnName = conditions[i];
      Object value = conditions[i + 1];

      if( !(columnName instanceof String)) {
        throw new RuntimeException("Each odd item should be a string with column name.");
      }

      if(value == null) {
        conditionList.add(escapeColumnName((String) columnName) + " IS NULL");
      } else {
        conditionList.add(escapeColumnName((String) columnName) + " = " + (escapeValues ? convertObjectToQueryString(value) : value));
      }
    }

    if(conditionList.size() != 0) {
      sb.append(" WHERE ").append(StringUtils.join(conditionList, " AND "));
    }

    return executeQuery(sb.toString());
  }

  /**
   * Convert given object to it's representation that can be safely used inside
   * query.
   *
   * @param value Value to convert
   * @return Query safe string representation
   */
  public String convertObjectToQueryString(Object value) {
    if(value == null) {
      return nullConstant();
    } else if(value.getClass() == String.class) {
      return escapeValueString((String)value);
    } else {
      return value.toString();
    }
  }

  /**
   * Drop table.
   *
   * Any exceptions will be ignored.
   *
   * @param tableName
   */
  public void dropTable(TableName tableName) {
    StringBuilder sb = new StringBuilder("DROP TABLE ");
    sb.append(getTableFragment(tableName));

    try {
      executeUpdate(sb.toString());
    } catch(RuntimeException e) {
      LOG.info("Ignoring exception: " + e);
    }
  }

  /**
   * Drop schema.
   *
   * Any exceptions will be ignored.
   *
   * @param schemaName
   */
  public void dropSchema(String schemaName) {
    StringBuilder sb = new StringBuilder("DROP SCHEMA ");
    sb.append(escapeSchemaName(schemaName));
    sb.append(" CASCADE");

    try {
      executeUpdate(sb.toString());
    } catch(RuntimeException e) {
      LOG.info("Ignoring exception: " + e);
    }
  }

  /**
   * Return number of rows from given table.
   *
   * @param tableName Table name
   * @return Number of rows
   */
  public long rowCount(TableName tableName) {
    StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ");
    sb.append(getTableFragment(tableName));

    ResultSet rs = null;
    try {
      rs = executeQuery(sb.toString());
      if(!rs.next()) {
        throw new RuntimeException("Row count query did not returned any rows.");
      }

      return rs.getLong(1);
    } catch (SQLException e) {
      LOG.error("Can't get number of rows: ", e);
      throw new RuntimeException("Can't get number of rows: ", e);
    } finally {
      closeResultSetWithStatement(rs);
    }
  }

  /**
   * Close given result set (if not null) and associated statement.
   *
   * @param rs ResultSet to close.
   */
  public void closeResultSetWithStatement(ResultSet rs) {
    if(rs != null) {
      try {
        Statement stmt = rs.getStatement();
        rs.close();
        stmt.close();
      } catch (SQLException e) {
        LOG.info("Ignoring exception: ", e);
      }
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

  /**
   * Dump content of given table to log.
   *
   * @param tableName Name of the table
   */
  public void dumpTable(TableName tableName) {
    String query = "SELECT * FROM " + getTableFragment(tableName);
    List<String> list = new LinkedList<String>();
    ResultSet rs = null;

    try {
      rs = executeQuery(query);

      // Header with column names
      ResultSetMetaData md = rs.getMetaData();
      for(int i = 0; i < md.getColumnCount(); i++) {
        list.add(md.getColumnName(i+1));
      }
      LOG.info("Dumping table " + tableName);
      LOG.info("|" + StringUtils.join(list, "|") + "|");

      // Table rows
      while(rs.next()) {
        list.clear();
        for(int i = 0; i < md.getColumnCount(); i++) {
          list.add(rs.getObject(i+1).toString());
        }
        LOG.info("|" + StringUtils.join(list, "|") + "|");
      }

    } catch (SQLException e) {
      LOG.info("Ignoring exception: ", e);
    } finally {
      closeResultSetWithStatement(rs);
    }
  }

  /**
   * Generated properly escaped table name fragment that can be used
   * in SQL query.
   *
   * @param tableName Full table name
   * @return
   */
  public String getTableFragment(TableName tableName) {
    if(tableName.getSchemaName() == null) {
      return escapeTableName(tableName.getTableName());
    }

    return escapeSchemaName(tableName.getSchemaName()) + "." + escapeTableName(tableName.getTableName());
  }
}

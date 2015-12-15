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
import org.apache.sqoop.common.test.db.types.DatabaseTypeList;
import org.apache.sqoop.common.test.db.types.DefaultTypeList;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
@edu.umd.cs.findbugs.annotations.SuppressWarnings
  ({"SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
    "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"})
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
    return new DefaultTypeList();
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

    try (Statement stmt = databaseConnection.createStatement()) {
      stmt.executeUpdate(query);
    } catch (SQLException e) {
      LOG.error("Error in executing query", e);
      throw new RuntimeException("Error in executing query", e);
    }
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
    try {
      StringBuilder sb = new StringBuilder("INSERT INTO ");
      sb.append(getTableFragment(tableName));
      sb.append(" VALUES (");

      for (int i = 0; i < values.length - 1; i++) {
        sb.append("?, ");
      }
      sb.append("?)");

      PreparedStatement statement = null;
      try {
        statement = databaseConnection.prepareStatement(sb.toString());
        for (int i = 0; i < values.length; i++) {
          insertObjectIntoPreparedStatement(statement, i +1, values[i]);
        }

        statement.executeUpdate();
      } finally {
        if (statement != null) {
          statement.close();
        }
      }
    } catch (SQLException sqlException) {
      throw new RuntimeException("can't insert row", sqlException);
    }

  }

  /**
   * Return rows that match given conditions.
   *
   * @param tableName Table name
   * @param conditions Conditions in form of double values - column name and value, for example: "id", 1 or "last_update_date", null
   * @return PreparedStatement representing the requested query
   */
  public PreparedStatement getRowsPreparedStatement(TableName tableName, Object[] conditions) {
    try {
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
          conditionList.add(escapeColumnName((String) columnName) + " = ?");
        }
      }

      if(conditionList.size() != 0) {
        sb.append(" WHERE ").append(StringUtils.join(conditionList, " AND "));
      }

      PreparedStatement preparedStatement = getConnection().prepareStatement(sb.toString());
      for(int i = 1; i < conditions.length; i += 2) {
        Object value = conditions[i];
        if (value != null) {
          insertObjectIntoPreparedStatement(preparedStatement, i, value);
        }
      }

      return preparedStatement;
    } catch (SQLException sqlException) {
      throw new RuntimeException("can't insert row", sqlException);
    }
  }

  private void insertObjectIntoPreparedStatement(PreparedStatement preparedStatement, int parameterIndex, Object value) throws SQLException {
    if (value instanceof String) {
      preparedStatement.setString(parameterIndex, (String) value);
    } else if (value instanceof Short) {
      preparedStatement.setShort(parameterIndex, ((Short) value).shortValue());
    } else if (value instanceof Integer) {
      preparedStatement.setInt(parameterIndex, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      preparedStatement.setLong(parameterIndex, ((Long) value).longValue());
    } else if (value instanceof Float) {
      preparedStatement.setFloat(parameterIndex, ((Float) value).floatValue());
    } else if (value instanceof Double) {
      preparedStatement.setDouble(parameterIndex, ((Double) value).doubleValue());
    } else if (value instanceof Boolean) {
      preparedStatement.setBoolean(parameterIndex, ((Boolean) value).booleanValue());
    } else if (value instanceof Byte) {
      preparedStatement.setByte(parameterIndex, ((Byte) value).byteValue());
    } else if (value instanceof Character) {
      preparedStatement.setString(parameterIndex, value.toString());
    } else if (value instanceof Timestamp) {
      preparedStatement.setString(parameterIndex, value.toString());
    } else if (value instanceof BigDecimal) {
      preparedStatement.setString(parameterIndex, value.toString());
    } else {
      preparedStatement.setObject(parameterIndex, value);
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

    try (Statement stmt = databaseConnection.createStatement(); ResultSet rs = stmt.executeQuery(sb.toString())) {
      if(!rs.next()) {
        throw new RuntimeException("Row count query did not returned any rows.");
      }

      return rs.getLong(1);
    } catch (SQLException e) {
      LOG.error("Can't get number of rows: ", e);
      throw new RuntimeException("Can't get number of rows: ", e);
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

    try (Statement stmt = databaseConnection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {

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

  /**
   * Drop database, this should be implemented for the DatabaseProvider like:
   * MySqlProvider.
   *
   * @param databaseName
   */
  public void dropDatabase(String databaseName) {
  }
}

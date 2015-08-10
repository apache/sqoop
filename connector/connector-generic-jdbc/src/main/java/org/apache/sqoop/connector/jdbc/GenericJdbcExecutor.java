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
package org.apache.sqoop.connector.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.GenericJdbcConnectorError;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.utils.ClassUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Database executor that is based on top of JDBC spec.
 */
public class GenericJdbcExecutor {

  private static final Logger LOG = Logger.getLogger(GenericJdbcExecutor.class);

  /**
   * Keys for JDBC properties
   *
   * We're following JDBC 4 spec:
   * http://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf?AuthParam=1426813649_0155f473b02dbca8bbd417dd061669d7
   */
  public static final String JDBC_PROPERTY_USERNAME = "user";
  public static final String JDBC_PROPERTY_PASSWORD = "password";

  /**
   * User configured link with credentials and such
   */
  private LinkConfiguration link;

  /**
   * Internal connection object (we'll hold to it)
   */
  private Connection connection;

  /**
   * Prepare statement
   */
  private PreparedStatement preparedStatement;

  public GenericJdbcExecutor(LinkConfiguration linkConfig) {
    // Persist link configuration for future use
    this.link = linkConfig;

    assert link != null;
    assert link.linkConfig != null;
    assert link.linkConfig.connectionString != null;

    // Load/register the JDBC driver to JVM
    Class driverClass = ClassUtils.loadClass(link.linkConfig.jdbcDriver);
    if(driverClass == null) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0000, link.linkConfig.jdbcDriver);
    }

    // Properties that we will use for the connection
    Properties properties = new Properties();
    if(link.linkConfig.jdbcProperties != null) {
      properties.putAll(link.linkConfig.jdbcProperties);
    }

    // Propagate username and password to the properties
    //
    // DriverManager have two relevant API for us:
    // * getConnection(url, username, password)
    // * getConnection(url, properties)
    // As we have to use properties, we need to use the later
    // method and hence we have to persist the credentials there.
    if(link.linkConfig.username != null) {
      properties.put(JDBC_PROPERTY_USERNAME, link.linkConfig.username);
    }
    if(link.linkConfig.password != null) {
      properties.put(JDBC_PROPERTY_PASSWORD, link.linkConfig.password);
    }

    // Finally create the connection
    try {
      connection = DriverManager.getConnection(link.linkConfig.connectionString, properties);
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0001, e);
    }

    // Fill in defaults if they were not pre-entered by user
    if(link.dialect.identifierEnclose == null) {
      link.dialect.identifierEnclose = "\"";
    }
  }

  public ResultSet executeQuery(String sql) {
    try {
      Statement statement = connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      return statement.executeQuery(sql);

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public PreparedStatement createStatement(String sql) {
     try {
      return connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void setAutoCommit(boolean autoCommit) {
    try {
      connection.setAutoCommit(autoCommit);
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  /**
   *  Enclose given identifier based on the configuration from user.
   *
   * @param identifier Identifier to enclose
   * @return Enclosed variant
   */
  public String encloseIdentifier(String identifier) {
    assert identifier != null;
    return link.dialect.identifierEnclose + identifier + link.dialect.identifierEnclose;
  }

  /**
   * Enclose multiple identifiers and join them together to one string.
   *
   * Used to convert (schema, table) to string "schema"."table" needed for SQL queries.
   *
   * @param identifiers  Identifiers to enclose
   * @return Enclose identifiers joined with "."
   */
  public String encloseIdentifiers(String ...identifiers) {
    assert identifiers != null;

    List<String> enclosedIdentifiers = new LinkedList<String>();
    for(String identifier: identifiers) {
      if(identifier != null) {
        enclosedIdentifiers.add(encloseIdentifier(identifier));
      }
    }

    return StringUtils.join(enclosedIdentifiers, ".");
  }

  /**
   * Create column list fragment for SELECT SQL.
   *
   * For example for (id, text, date), will automatically escape the column
   * names and return one string:
   *   "id", "text", "date"
   *
   * This method won't work correctly if the column name contains an expression
   * or anything else beyond just a column name.
   *
   * @param columnNames Column names to escape and join.
   * @return
   */
  public String columnList(String ...columnNames) {
    assert columnNames != null;

    List<String> escapedColumns = new LinkedList<String>();
    for(String column : columnNames) {
      escapedColumns.add(encloseIdentifier(column));
    }

    return StringUtils.join(escapedColumns, ", ");
  }

  public void deleteTableData(String tableName) {
    LOG.info("Deleting all the rows from: " + tableName);
    executeUpdate("DELETE FROM " + encloseIdentifier(tableName));
  }

  public void migrateData(String fromTable, String toTable) {
    String insertQuery = "INSERT INTO " + encloseIdentifier(toTable) + " SELECT * FROM " + encloseIdentifier(fromTable);
    Statement stmt = null;
    Boolean oldAutoCommit = null;
    try {
      final long expectedInsertCount = getTableRowCount(fromTable);
      oldAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      stmt = connection.createStatement();
      final int actualInsertCount = stmt.executeUpdate(insertQuery);
      if(expectedInsertCount == actualInsertCount) {
        LOG.info("Transferred " + actualInsertCount + " rows of staged data " +
          "from: " + fromTable + " to: " + toTable);
        connection.commit();
        deleteTableData(fromTable);
        connection.commit();
      } else {
        LOG.error("Rolling back as number of rows inserted into table: " +
          toTable + " was: " + actualInsertCount + " expected: " +
          expectedInsertCount);
        connection.rollback();
        throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0018);
      }
    } catch(SQLException e) {
      logSQLException(e, "Got SQLException while migrating data from: " + fromTable + " to: " + toTable);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0018, e);
    } finally {
      if(stmt != null) {
        try {
          stmt.close();
        } catch(SQLException e) {
          logSQLException(e, "Got SQLException at the time of closing statement.");
        }
      }
      if(oldAutoCommit != null) {
        try {
          connection.setAutoCommit(oldAutoCommit);
        } catch(SQLException e) {
          logSQLException(e, "Got SQLException while setting autoCommit mode.");
        }
      }
    }
  }

  public long getTableRowCount(String tableName) {
    ResultSet resultSet = executeQuery("SELECT COUNT(1) FROM " + encloseIdentifier(tableName));
    try {
      resultSet.next();
      return resultSet.getLong(1);
    } catch(SQLException e) {
      throw new SqoopException(
        GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0004, e);
    } finally {
      try {
        if(resultSet != null)
          resultSet.close();
      } catch(SQLException e) {
        logSQLException(e, "Got SQLException while closing resultset.");
      }
    }
  }

  public void executeUpdate(String sql) {
    try {
      Statement statement = connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate(sql);

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void beginBatch(String sql) {
    try {
      preparedStatement = connection.prepareStatement(sql,
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void addBatch(Object[] array, Schema schema) {
    try {
      Column[] schemaColumns = schema.getColumnsArray();
      for (int i = 0; i < array.length; i++) {
        Column schemaColumn = schemaColumns[i];
        switch (schemaColumn.getType()) {
        case DATE:
          // convert the JODA date to sql date
          LocalDate date = (LocalDate) array[i];
          java.sql.Date sqlDate = new java.sql.Date(date.toDateTimeAtCurrentTime().getMillis());
          preparedStatement.setObject(i + 1, sqlDate);
          break;
        case DATE_TIME:
          // convert the JODA date time to sql date
          DateTime dateTime = (DateTime) array[i];
          Timestamp timestamp = new Timestamp(dateTime.getMillis());
          preparedStatement.setObject(i + 1, timestamp);
          break;
        case TIME:
          // convert the JODA time to sql date
          LocalTime time = (LocalTime) array[i];
          java.sql.Time sqlTime = new java.sql.Time(time.toDateTimeToday().getMillis());
          preparedStatement.setObject(i + 1, sqlTime);
          break;
        default:
          // for anything else
          preparedStatement.setObject(i + 1, array[i]);
        }
      }
      preparedStatement.addBatch();
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void executeBatch(boolean commit) {
    try {
      preparedStatement.executeBatch();
      if (commit) {
        connection.commit();
      }
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void endBatch() {
    try {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  /**
   * Return primary key for given table.
   *
   * @param identifiers Identifiers that are used to build the table's name. Following
   *                    variants are accepted:
   *                    * (catalog, schema, table)
   *                    * (schema, table)
   *                    * (table)
   *                    Return value of any combination is "undefined".
   * @return Primary key's name
   */
  public String getPrimaryKey(String ...identifiers) {
    int index = 0;
    String catalog = identifiers.length >= 3 ? identifiers[index++] : null;
    String schema = identifiers.length >= 2 ? identifiers[index++] : null;
    String table = identifiers[index];

    try {
      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet rs = dbmd.getPrimaryKeys(catalog, schema, table);

      if (rs != null && rs.next()) {
        return rs.getString("COLUMN_NAME");

      } else {
        return null;
      }

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
    }
  }

  public String[] getQueryColumns(String query) {
    try {
      Statement statement = connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ResultSet rs = statement.executeQuery(query);

      ResultSetMetaData rsmd = rs.getMetaData();
      int count = rsmd.getColumnCount();
      String[] columns = new String[count];
      for (int i = 0; i < count; i++) {
        columns[i] = rsmd.getColumnName(i+1);
      }

      return columns;

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
    }
  }

  /**
   * Verifies existence of table in the database.
   *
   * @param identifiers Identifiers that are used to build the table's name. Following
   *                    variants are accepted:
   *                    * (catalog, schema, table)
   *                    * (schema, table)
   *                    * (table)
   *                    Return value of any combination is "undefined".
   * @return True if given table exists
   */
  public boolean existTable(String ...identifiers) {
    int index = 0;
    String catalog = identifiers.length >= 3 ? identifiers[index++] : null;
    String schema = identifiers.length >= 2 ? identifiers[index++] : null;
    String table = identifiers[index];

    try {
      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet rs = dbmd.getTables(catalog, schema, table, null);

      if (rs.next()) {
        return true;
      } else {
        return false;
      }

    } catch (SQLException e) {
      logSQLException(e);
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
    }
  }

  public void close() {
    try {
      connection.close();

    } catch (SQLException e) {
      logSQLException(e);
    }
  }

  private void logSQLException(SQLException e) {
    logSQLException(e, "Caught SQLException:");
  }

  private void logSQLException(SQLException e, String message) {
    LOG.error(message, e);
    if(e.getNextException() != null) {
      logSQLException(e.getNextException(), "Caused by:");
    }
  }
}

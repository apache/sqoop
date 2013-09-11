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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;

public class GenericJdbcExecutor {

  private static final Logger LOG =
    Logger.getLogger(GenericJdbcExecutor.class);

  private Connection connection;
  private PreparedStatement preparedStatement;

  public GenericJdbcExecutor(String driver, String url,
      String username, String password) {
    try {
      Class.forName(driver);
      connection = DriverManager.getConnection(url, username, password);

    } catch (ClassNotFoundException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0000, driver, e);

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0001, e);
    }
  }

  public ResultSet executeQuery(String sql) {
    try {
      Statement statement = connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      return statement.executeQuery(sql);

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void setAutoCommit(boolean autoCommit) {
    try {
      connection.setAutoCommit(autoCommit);
    } catch (SQLException e) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void deleteTableData(String tableName) {
    LOG.info("Deleting all the rows from: " + tableName);
    executeUpdate("DELETE FROM " + tableName);
  }

  public void migrateData(String fromTable, String toTable) {
    String insertQuery = "INSERT INTO " + toTable +
      " ( SELECT * FROM " + fromTable + " )";
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
      LOG.error("Got SQLException while migrating data from: " + fromTable +
        " to: " + toTable, e);
      throw new SqoopException(
        GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0018, e);
    } finally {
      if(stmt != null) {
        try {
          stmt.close();
        } catch(SQLException e) {
          LOG.warn("Got SQLException at the time of closing statement.", e);
        }
      }
      if(oldAutoCommit != null) {
        try {
          connection.setAutoCommit(oldAutoCommit);
        } catch(SQLException e) {
          LOG.warn("Got SQLException while setting autoCommit mode.", e);
        }
      }
    }
  }

  public long getTableRowCount(String tableName) {
    ResultSet resultSet = executeQuery("SELECT COUNT(1) FROM " + tableName);
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
        LOG.warn("Got SQLException while closing resultset.", e);
      }
    }
  }

  public void executeUpdate(String sql) {
    try {
      Statement statement = connection.createStatement(
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.executeUpdate(sql);

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void beginBatch(String sql) {
    try {
      preparedStatement = connection.prepareStatement(sql,
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void addBatch(Object[] array) {
    try {
      for (int i=0; i<array.length; i++) {
        preparedStatement.setObject(i+1, array[i]);
      }
      preparedStatement.addBatch();
    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void executeBatch(boolean commit) {
    try {
      preparedStatement.executeBatch();
      if (commit) {
        connection.commit();
      }
    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public void endBatch() {
    try {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0002, e);
    }
  }

  public String getPrimaryKey(String table) {
    try {
      String[] splitNames = dequalify(table);
      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet rs = dbmd.getPrimaryKeys(null, splitNames[0], splitNames[1]);

      if (rs != null && rs.next()) {
        return rs.getString("COLUMN_NAME");

      } else {
        return null;
      }

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
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
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
    }
  }

  public boolean existTable(String table) {
    try {
      String[] splitNames = dequalify(table);

      DatabaseMetaData dbmd = connection.getMetaData();
      ResultSet rs = dbmd.getTables(null, splitNames[0], splitNames[1], null);

      if (rs.next()) {
        return true;
      } else {
        return false;
      }

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0003, e);
    }
  }

  /*
   * If not qualified already, the name will be added with the qualifier.
   * If qualified already, old qualifier will be replaced.
   */
  public String qualify(String name, String qualifier) {
    String[] splits = dequalify(name);
    return qualifier + "." + splits[1];
  }

  /*
   * Split the name into a qualifier (element 0) and a base (element 1).
   */
  public String[] dequalify(String name) {
    String qualifier;
    String base;
    int dot = name.indexOf(".");
    if (dot != -1) {
      qualifier = name.substring(0, dot);
      base = name.substring(dot + 1);
    } else {
      qualifier = null;
      base = name;
    }
    return new String[] {qualifier, base};
  }

  public String delimitIdentifier(String name) {
    return name;
  }

  public void close() {
    try {
      connection.close();

    } catch (SQLException e) {
      // TODO: Log the exception
    }
  }

}

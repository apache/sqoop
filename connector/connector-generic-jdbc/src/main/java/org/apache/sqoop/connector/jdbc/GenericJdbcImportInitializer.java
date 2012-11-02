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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcImportInitializer extends Initializer {

  private static final Logger LOG =
    Logger.getLogger(GenericJdbcImportInitializer.class);

  private GenericJdbcExecutor executor;

  @Override
  public void initialize(MutableMapContext context, Object oConnectionConfig, Object oJobConfig) {
    ConnectionConfiguration connectionConfig = (ConnectionConfiguration)oConnectionConfig;
    ImportJobConfiguration jobConfig = (ImportJobConfiguration)oJobConfig;

    configureJdbcProperties(context, connectionConfig, jobConfig);

    try {
      configurePartitionProperties(context, connectionConfig, jobConfig);
      configureTableProperties(context, connectionConfig, jobConfig);

    } finally {
      executor.close();
    }
  }

  @Override
  public List<String> getJars(MapContext context, Object connectionConfiguration, Object jobConfiguration) {
    List<String> jars = new LinkedList<String>();

    ConnectionConfiguration connection = (ConnectionConfiguration) connectionConfiguration;
    jars.add(ClassUtils.jarForClass(connection.jdbcDriver));

    return jars;
  }

  private void configureJdbcProperties(MutableMapContext context, ConnectionConfiguration connectionConfig, ImportJobConfiguration jobConfig) {
    String driver = connectionConfig.jdbcDriver;
    String url = connectionConfig.connectionString;
    String username = connectionConfig.username;
    String password = connectionConfig.password;

    // TODO(jarcec): Those checks should be in validator and not here
    if (driver == null) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0012,
          GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER);
    }
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_DRIVER,
        driver);

    if (url == null) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0012,
          GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING);
    }
    context.setString(
        GenericJdbcConnectorConstants.CONNECTOR_JDBC_URL,
        url);

    if (username != null) {
      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_USERNAME,
          username);
    }

    if (password != null) {
      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_PASSWORD,
          password);
    }

    executor = new GenericJdbcExecutor(driver, url, username, password);
  }

  private void configurePartitionProperties(MutableMapContext context, ConnectionConfiguration connectionConfig, ImportJobConfiguration jobConfig) {
    // ----- configure column name -----

    String partitionColumnName = connectionConfig.partitionColumn;

    if (partitionColumnName == null) {
      // if column is not specified by the user,
      // find the primary key of the table (when there is a table).
      String tableName = connectionConfig.tableName;
      if (tableName != null) {
        partitionColumnName = executor.getPrimaryKey(tableName);
      }
    }

    if (partitionColumnName != null) {
      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME,
          partitionColumnName);

    } else {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0005);
    }

    // ----- configure column type, min value, and max value -----

    String minMaxQuery = connectionConfig.boundaryQuery;

    if (minMaxQuery == null) {
      StringBuilder builder = new StringBuilder();

      String tableName = connectionConfig.tableName;
      String tableSql = connectionConfig.sql;

      if (tableName != null && tableSql != null) {
        // when both table name and table sql are specified:
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

      } else if (tableName != null) {
        // when table name is specified:
        String column = partitionColumnName;
        builder.append("SELECT MIN(");
        builder.append(column);
        builder.append("), MAX(");
        builder.append(column);
        builder.append(") FROM ");
        builder.append(executor.delimitIdentifier(tableName));

      } else if (tableSql != null) {
        String column = executor.qualify(
            partitionColumnName, GenericJdbcConnectorConstants.SUBQUERY_ALIAS);
        builder.append("SELECT MIN(");
        builder.append(column);
        builder.append("), MAX(");
        builder.append(column);
        builder.append(") FROM ");
        builder.append("(");
        builder.append(tableSql.replace(
            GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 1"));
        builder.append(") ");
        builder.append(GenericJdbcConnectorConstants.SUBQUERY_ALIAS);

      } else {
        // when neither are specified:
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0008);
      }

      minMaxQuery = builder.toString();
    }


    LOG.debug("Using minMaxQuery: " + minMaxQuery);
    ResultSet rs = executor.executeQuery(minMaxQuery);
    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      if (rsmd.getColumnCount() != 2) {
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0006);
      }

      rs.next();

      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE,
          String.valueOf(rsmd.getColumnType(1)));
      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE,
          rs.getString(1));
      context.setString(
          GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE,
          rs.getString(2));

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0006, e);
    }
  }

  private void configureTableProperties(MutableMapContext context, ConnectionConfiguration connectionConfig, ImportJobConfiguration jobConfig) {
    String dataSql;
    String fieldNames;
    String outputDirectory;

    String tableName = connectionConfig.tableName;
    String tableSql = connectionConfig.sql;
    String tableColumns = connectionConfig.columns;

    //TODO(jarcec): Why is connector concerned with data directory? It should not need it at all!
    String datadir = connectionConfig.dataDirectory;
    String warehouse = connectionConfig.warehouse;
    if (warehouse == null) {
      warehouse = GenericJdbcConnectorConstants.DEFAULT_WAREHOUSE;
    } else if (!warehouse.endsWith(GenericJdbcConnectorConstants.FILE_SEPARATOR)) {
      warehouse += GenericJdbcConnectorConstants.FILE_SEPARATOR;
    }

    if (tableName != null && tableSql != null) {
      // when both table name and table sql are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

    } else if (tableName != null) {
      // when table name is specified:

      if (tableColumns == null) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ");
        builder.append(executor.delimitIdentifier(tableName));
        builder.append(" WHERE ");
        builder.append(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);
        dataSql = builder.toString();

        String[] queryColumns = executor.getQueryColumns(dataSql.replace(
            GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0"));
        fieldNames = StringUtils.join(queryColumns, ',');

      } else {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        builder.append(tableColumns);
        builder.append(" FROM ");
        builder.append(executor.delimitIdentifier(tableName));
        builder.append(" WHERE ");
        builder.append(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);
        dataSql = builder.toString();

        fieldNames = tableColumns;
      }

      if (datadir == null) {
        outputDirectory = warehouse + tableName;
      } else {
        outputDirectory = warehouse + datadir;
      }

    } else if (tableSql != null) {
      // when table sql is specified:

      if (tableSql.indexOf(
          GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN) == -1) {
        // make sure substitute token for conditions is in the specified sql
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0010);
      }

      if (tableColumns == null) {
        dataSql = tableSql;

        String[] queryColumns = executor.getQueryColumns(dataSql.replace(
            GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0"));
        fieldNames = StringUtils.join(queryColumns, ',');

      } else {
        String[] columns = StringUtils.split(tableColumns, ',');
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        builder.append(executor.qualify(
            columns[0], GenericJdbcConnectorConstants.SUBQUERY_ALIAS));
        for (int i = 1; i < columns.length; i++) {
          builder.append(",");
          builder.append(executor.qualify(
              columns[i], GenericJdbcConnectorConstants.SUBQUERY_ALIAS));
        }
        builder.append(" FROM ");
        builder.append("(");
        builder.append(tableSql);
        builder.append(") ");
        builder.append(GenericJdbcConnectorConstants.SUBQUERY_ALIAS);
        dataSql = builder.toString();

        fieldNames = tableColumns;
      }

      if (datadir == null) {
        outputDirectory =
            warehouse + GenericJdbcConnectorConstants.DEFAULT_DATADIR;
      } else {
        outputDirectory = warehouse + datadir;
      }

    } else {
      // when neither are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0008);
    }

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        dataSql.toString());
    context.setString(Constants.JOB_ETL_FIELD_NAMES, fieldNames);
    context.setString(Constants.JOB_ETL_OUTPUT_DIRECTORY, outputDirectory);
  }

}

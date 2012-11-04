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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ExportJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ImportJobConfiguration;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcExportInitializer extends Initializer {

  private GenericJdbcExecutor executor;

  @Override
  public void initialize(MutableContext context, Object connectionConfiguration, Object jobConfiguration) {
    ConnectionConfiguration connectionConfig = (ConnectionConfiguration)connectionConfiguration;
    ExportJobConfiguration jobConfig = (ExportJobConfiguration)jobConfiguration;

    configureJdbcProperties(context, connectionConfig, jobConfig);
    try {
      configureTableProperties(context, connectionConfig, jobConfig);

    } finally {
      executor.close();
    }
  }

  @Override
  public List<String> getJars(ImmutableContext context, Object connectionConfiguration, Object jobConfiguration) {
    List<String> jars = new LinkedList<String>();

    ConnectionConfiguration connection = (ConnectionConfiguration) connectionConfiguration;
    jars.add(ClassUtils.jarForClass(connection.jdbcDriver));

    return jars;
  }

  private void configureJdbcProperties(MutableContext context, ConnectionConfiguration connectionConfig, ExportJobConfiguration jobConfig) {
    String driver = connectionConfig.jdbcDriver;
    String url = connectionConfig.connectionString;
    String username = connectionConfig.username;
    String password = connectionConfig.password;

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

  private void configureTableProperties(MutableContext context, ConnectionConfiguration connectionConfig, ExportJobConfiguration jobConfig) {
    String dataSql;
    String inputDirectory;

    String tableName = connectionConfig.tableName;
    String tableSql = connectionConfig.sql;
    String tableColumns = connectionConfig.columns;

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
        String[] columns = executor.getQueryColumns("SELECT * FROM "
            + executor.delimitIdentifier(tableName) + " WHERE 1 = 0");
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(executor.delimitIdentifier(tableName));
        builder.append(" VALUES (?");
        for (int i = 1; i < columns.length; i++) {
          builder.append(",?");
        }
        builder.append(")");
        dataSql = builder.toString();

      } else {
        String[] columns = StringUtils.split(tableColumns, ',');
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(executor.delimitIdentifier(tableName));
        builder.append(" (");
        builder.append(tableColumns);
        builder.append(") VALUES (?");
        for (int i = 1; i < columns.length; i++) {
          builder.append(",?");
        }
        builder.append(")");
        dataSql = builder.toString();
      }

      if (datadir == null) {
        inputDirectory = warehouse + tableName;
      } else {
        inputDirectory = warehouse + datadir;
      }

    } else if (tableSql != null) {
      // when table sql is specified:

      if (tableSql.indexOf(
          GenericJdbcConnectorConstants.SQL_PARAMETER_MARKER) == -1) {
        // make sure parameter marker is in the specified sql
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0013);
      }

      if (tableColumns == null) {
        dataSql = tableSql;
      } else {
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0014);
      }

      if (datadir == null) {
        inputDirectory =
            warehouse + GenericJdbcConnectorConstants.DEFAULT_DATADIR;
      } else {
        inputDirectory = warehouse + datadir;
      }

    } else {
      // when neither are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0008);
    }

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        dataSql.toString());
    context.setString(Constants.JOB_ETL_INPUT_DIRECTORY, inputDirectory);
  }

}

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
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ExportJobConfiguration;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcExportInitializer extends Initializer<ConnectionConfiguration, ExportJobConfiguration> {

  private GenericJdbcExecutor executor;

  @Override
  public void initialize(InitializerContext context, ConnectionConfiguration connection, ExportJobConfiguration job) {
    configureJdbcProperties(context.getContext(), connection, job);
    try {
      configureTableProperties(context.getContext(), connection, job);
    } finally {
      executor.close();
    }
  }

  @Override
  public List<String> getJars(InitializerContext context, ConnectionConfiguration connection, ExportJobConfiguration job) {
    List<String> jars = new LinkedList<String>();

    jars.add(ClassUtils.jarForClass(connection.connection.jdbcDriver));

    return jars;
  }

  private void configureJdbcProperties(MutableContext context, ConnectionConfiguration connectionConfig, ExportJobConfiguration jobConfig) {
    String driver = connectionConfig.connection.jdbcDriver;
    String url = connectionConfig.connection.connectionString;
    String username = connectionConfig.connection.username;
    String password = connectionConfig.connection.password;

    assert driver != null;
    assert url != null;

    executor = new GenericJdbcExecutor(driver, url, username, password);
  }

  private void configureTableProperties(MutableContext context, ConnectionConfiguration connectionConfig, ExportJobConfiguration jobConfig) {
    String dataSql;

    String schemaName = jobConfig.table.schemaName;
    String tableName = jobConfig.table.tableName;
    String tableSql = jobConfig.table.sql;
    String tableColumns = jobConfig.table.columns;

    if (tableName != null && tableSql != null) {
      // when both table name and table sql are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

    } else if (tableName != null) {
      // when table name is specified:

      // For databases that support schemas (IE: postgresql).
      String fullTableName = (schemaName == null) ? executor.delimitIdentifier(tableName) : executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

      if (tableColumns == null) {
        String[] columns = executor.getQueryColumns("SELECT * FROM "
            + fullTableName + " WHERE 1 = 0");
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(fullTableName);
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
        builder.append(fullTableName);
        builder.append(" (");
        builder.append(tableColumns);
        builder.append(") VALUES (?");
        for (int i = 1; i < columns.length; i++) {
          builder.append(",?");
        }
        builder.append(")");
        dataSql = builder.toString();
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
    } else {
      // when neither are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0008);
    }

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL,
        dataSql.toString());
  }
}

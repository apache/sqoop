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
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.jdbc.util.SqlTypesUtils;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcToInitializer extends Initializer<ConnectionConfiguration, ToJobConfiguration> {

  private GenericJdbcExecutor executor;
  private static final Logger LOG =
    Logger.getLogger(GenericJdbcToInitializer.class);

  @Override
  public void initialize(InitializerContext context, ConnectionConfiguration connection, ToJobConfiguration job) {
    configureJdbcProperties(context.getContext(), connection, job);
    try {
      configureTableProperties(context.getContext(), connection, job);
    } finally {
      executor.close();
    }
  }

  @Override
  public List<String> getJars(InitializerContext context, ConnectionConfiguration connection, ToJobConfiguration job) {
    List<String> jars = new LinkedList<String>();

    jars.add(ClassUtils.jarForClass(connection.connection.jdbcDriver));

    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context, ConnectionConfiguration connectionConfiguration, ToJobConfiguration toJobConfiguration) {
    configureJdbcProperties(context.getContext(), connectionConfiguration, toJobConfiguration);

    String schemaName = toJobConfiguration.toTable.tableName;

    if (schemaName == null) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0019,
          "Table name extraction not supported yet.");
    }

    if(toJobConfiguration.toTable.schemaName != null) {
      schemaName = toJobConfiguration.toTable.schemaName + "." + schemaName;
    }

    Schema schema = new Schema(schemaName);
    ResultSet rs = null;
    ResultSetMetaData rsmt = null;
    try {
      rs = executor.executeQuery("SELECT * FROM " + schemaName + " WHERE 1 = 0");

      rsmt = rs.getMetaData();
      for (int i = 1 ; i <= rsmt.getColumnCount(); i++) {
        Column column = SqlTypesUtils.sqlTypeToAbstractType(rsmt.getColumnType(i));

        String columnName = rsmt.getColumnName(i);
        if (columnName == null || columnName.equals("")) {
          columnName = rsmt.getColumnLabel(i);
          if (null == columnName) {
            columnName = "Column " + i;
          }
        }

        column.setName(columnName);
        schema.addColumn(column);
      }

      return schema;
    } catch (SQLException e) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0016, e);
    } finally {
      if(rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOG.info("Ignoring exception while closing ResultSet", e);
        }
      }
    }
  }

  private void configureJdbcProperties(MutableContext context, ConnectionConfiguration connectionConfig, ToJobConfiguration jobConfig) {
    String driver = connectionConfig.connection.jdbcDriver;
    String url = connectionConfig.connection.connectionString;
    String username = connectionConfig.connection.username;
    String password = connectionConfig.connection.password;

    assert driver != null;
    assert url != null;

    executor = new GenericJdbcExecutor(driver, url, username, password);
  }

  private void configureTableProperties(MutableContext context, ConnectionConfiguration connectionConfig, ToJobConfiguration jobConfig) {
    String dataSql;

    String schemaName = jobConfig.toTable.schemaName;
    String tableName = jobConfig.toTable.tableName;
    String stageTableName = jobConfig.toTable.stageTableName;
    boolean clearStageTable = jobConfig.toTable.clearStageTable == null ?
      false : jobConfig.toTable.clearStageTable;
    final boolean stageEnabled =
      stageTableName != null && stageTableName.length() > 0;
    String tableSql = jobConfig.toTable.sql;
    String tableColumns = jobConfig.toTable.columns;

    if (tableName != null && tableSql != null) {
      // when both fromTable name and fromTable sql are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

    } else if (tableName != null) {
      // when fromTable name is specified:
      if(stageEnabled) {
        LOG.info("Stage has been enabled.");
        LOG.info("Use stageTable: " + stageTableName +
          " with clearStageTable: " + clearStageTable);

        if(clearStageTable) {
          executor.deleteTableData(stageTableName);
        } else {
          long stageRowCount = executor.getTableRowCount(stageTableName);
          if(stageRowCount > 0) {
            throw new SqoopException(
              GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0017);
          }
        }
      }

      // For databases that support schemas (IE: postgresql).
      final String tableInUse = stageEnabled ? stageTableName : tableName;
      String fullTableName = (schemaName == null) ?
        executor.delimitIdentifier(tableInUse) :
        executor.delimitIdentifier(schemaName) +
          "." + executor.delimitIdentifier(tableInUse);

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
      // when fromTable sql is specified:

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

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_TO_DATA_SQL,
        dataSql);
  }
}

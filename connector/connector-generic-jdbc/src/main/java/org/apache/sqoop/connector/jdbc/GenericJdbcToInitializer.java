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
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.jdbc.util.SqlTypesUtils;
import org.apache.sqoop.error.code.GenericJdbcConnectorError;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcToInitializer extends Initializer<LinkConfiguration, ToJobConfiguration> {

  private GenericJdbcExecutor executor;
  private static final Logger LOG =
    Logger.getLogger(GenericJdbcToInitializer.class);

  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    executor = new GenericJdbcExecutor(linkConfig.linkConfig);
    try {
      configureTableProperties(context.getContext(), linkConfig, toJobConfig);
    } finally {
      executor.close();
    }
  }

  @Override
  public Set<String> getJars(InitializerContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, toJobConfig);
    jars.add(ClassUtils.jarForClass(linkConfig.linkConfig.jdbcDriver));
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    executor = new GenericJdbcExecutor(linkConfig.linkConfig);

    String schemaName = toJobConfig.toJobConfig.tableName;

    if (schemaName == null) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0019,
          "Table name extraction not supported yet.");
    }

    if(toJobConfig.toJobConfig.schemaName != null) {
      schemaName = toJobConfig.toJobConfig.schemaName + "." + schemaName;
    }

    Schema schema = new Schema(schemaName);
    ResultSet rs = null;
    ResultSetMetaData rsmt = null;
    try {
      rs = executor.executeQuery("SELECT * FROM " + schemaName + " WHERE 1 = 0");

      rsmt = rs.getMetaData();
      for (int i = 1 ; i <= rsmt.getColumnCount(); i++) {
        String columnName = rsmt.getColumnName(i);

        if (StringUtils.isEmpty(columnName)) {
          columnName = rsmt.getColumnLabel(i);
          if (StringUtils.isEmpty(columnName)) {
            columnName = "Column " + i;
          }
        }

        Column column = SqlTypesUtils.sqlTypeToSchemaType(rsmt.getColumnType(i), columnName, rsmt.getPrecision(i), rsmt.getScale(i));
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

  private void configureTableProperties(MutableContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) {
    String dataSql;

    String schemaName = toJobConfig.toJobConfig.schemaName;
    String tableName = toJobConfig.toJobConfig.tableName;
    String stageTableName = toJobConfig.toJobConfig.stageTableName;
    boolean clearStageTable = toJobConfig.toJobConfig.shouldClearStageTable == null ?
      false : toJobConfig.toJobConfig.shouldClearStageTable;
    final boolean stageEnabled =
      stageTableName != null && stageTableName.length() > 0;
    String tableSql = toJobConfig.toJobConfig.sql;
    String tableColumns = toJobConfig.toJobConfig.columns;

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

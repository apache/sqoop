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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.util.SqlTypesUtils;
import org.apache.sqoop.error.code.GenericJdbcConnectorError;
import org.apache.sqoop.job.Constants;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.utils.ClassUtils;

public class GenericJdbcFromInitializer extends Initializer<LinkConfiguration, FromJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(GenericJdbcFromInitializer.class);

  private GenericJdbcExecutor executor;

  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    executor = new GenericJdbcExecutor(linkConfig.linkConfig);

    try {
      configurePartitionProperties(context.getContext(), linkConfig, fromJobConfig);
      configureTableProperties(context.getContext(), linkConfig, fromJobConfig);
    } catch(SQLException e) {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0016, e);
    } finally {
      executor.close();
    }
  }

  @Override
  public Set<String> getJars(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    Set<String> jars = super.getJars(context, linkConfig, fromJobConfig);
    jars.add(ClassUtils.jarForClass(linkConfig.linkConfig.jdbcDriver));
    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    executor = new GenericJdbcExecutor(linkConfig.linkConfig);

    String schemaName = fromJobConfig.fromJobConfig.tableName;
    if(schemaName == null) {
      schemaName = "Query";
    } else if(fromJobConfig.fromJobConfig.schemaName != null) {
      schemaName = fromJobConfig.fromJobConfig.schemaName + "." + schemaName;
    }

    Schema schema = new Schema(schemaName);
    ResultSet rs = null;
    ResultSetMetaData rsmt = null;
    try {
      rs = executor.executeQuery(
        context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL)
          .replace(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0")
      );

      rsmt = rs.getMetaData();
      for (int i = 1 ; i <= rsmt.getColumnCount(); i++) {
        String columnName = rsmt.getColumnLabel(i);
        if (StringUtils.isEmpty(columnName)) {
          columnName = rsmt.getColumnName(i);
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
      if (executor != null) {
        executor.close();
      }
    }
  }

  private void configurePartitionProperties(MutableContext context, LinkConfiguration linkConfig, FromJobConfiguration jobConf) throws SQLException {
    // Assertions that should be valid (verified via validator)
    assert (jobConf.fromJobConfig.tableName != null && jobConf.fromJobConfig.sql == null) ||
           (jobConf.fromJobConfig.tableName == null && jobConf.fromJobConfig.sql != null);
    assert (jobConf.fromJobConfig.boundaryQuery == null && jobConf.incrementalRead.checkColumn == null) ||
           (jobConf.fromJobConfig.boundaryQuery != null && jobConf.incrementalRead.checkColumn == null) ||
           (jobConf.fromJobConfig.boundaryQuery == null && jobConf.incrementalRead.checkColumn != null);

    // We have few if/else conditions based on import type
    boolean tableImport = jobConf.fromJobConfig.tableName != null;
    boolean incrementalImport = jobConf.incrementalRead.checkColumn != null;

    // For generating queries
    StringBuilder sb = new StringBuilder();

    // Partition column name
    String partitionColumnName = jobConf.fromJobConfig.partitionColumn;
    // If it's not specified, we can use primary key of given table (if it's table based import)
    if (StringUtils.isBlank(partitionColumnName) && tableImport) {
        partitionColumnName = executor.getPrimaryKey(jobConf.fromJobConfig.tableName);
    }
    // If we don't have partition column name, we will error out
    if (partitionColumnName != null) {
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME, partitionColumnName);
    } else {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0005);
    }
    LOG.info("Using partition column: " + partitionColumnName);

    // From fragment for subsequent queries
    String fromFragment;
    if(tableImport) {
      String tableName = jobConf.fromJobConfig.tableName;
      String schemaName = jobConf.fromJobConfig.schemaName;

      fromFragment = executor.delimitIdentifier(tableName);
      if(schemaName != null) {
        fromFragment = executor.delimitIdentifier(schemaName) + "." + fromFragment;
      }
    } else {
      sb.setLength(0);
      sb.append("(");
      sb.append(jobConf.fromJobConfig.sql.replace(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 1"));
      sb.append(") ");
      sb.append(GenericJdbcConnectorConstants.SUBQUERY_ALIAS);
      fromFragment = sb.toString();
    }

    // If this is incremental, then we need to get new maximal value and persist is a constant
    String incrementalMaxValue = null;
    if(incrementalImport) {
      sb.setLength(0);
      sb.append("SELECT ");
      sb.append("MAX(").append(jobConf.incrementalRead.checkColumn).append(") ");
      sb.append("FROM ");
      sb.append(fromFragment);

      String incrementalNewMaxValueQuery = sb.toString();
      LOG.info("Incremental new max value query:  " + incrementalNewMaxValueQuery);

      ResultSet rs = null;
      try {
        rs = executor.executeQuery(incrementalNewMaxValueQuery);

        if (!rs.next()) {
          throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0022);
        }

        incrementalMaxValue = rs.getString(1);
        context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE, incrementalMaxValue);
        LOG.info("New maximal value for incremental import is " + incrementalMaxValue);
      } finally {
        if(rs != null) {
          rs.close();
        }
      }
    }

    // Retrieving min and max values for partition column
    String minMaxQuery = jobConf.fromJobConfig.boundaryQuery;
    if (minMaxQuery == null) {
      sb.setLength(0);
      sb.append("SELECT ");
      sb.append("MIN(").append(partitionColumnName).append("), ");
      sb.append("MAX(").append(partitionColumnName).append(") ");
      sb.append("FROM ").append(fromFragment).append(" ");

      if(incrementalImport) {
        sb.append("WHERE ");
        sb.append(jobConf.incrementalRead.checkColumn).append(" > ?");
        sb.append(" AND ");
        sb.append(jobConf.incrementalRead.checkColumn).append(" <= ?");
      }

      minMaxQuery = sb.toString();
    }
    LOG.info("Using min/max query: " + minMaxQuery);

    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = executor.createStatement(minMaxQuery);
      if (incrementalImport) {
        ps.setString(1, jobConf.incrementalRead.lastValue);
        ps.setString(2, incrementalMaxValue);
      }

      rs = ps.executeQuery();
      if(!rs.next()) {
        throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0006);
      }

      // Boundaries for the job
      String min = rs.getString(1);
      String max = rs.getString(2);

      // Type of the partition column
      ResultSetMetaData rsmd = rs.getMetaData();
      if (rsmd.getColumnCount() != 2) {
        throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0006);
      }
      int columnType = rsmd.getColumnType(1);

      LOG.info("Boundaries for the job: min=" + min + ", max=" + max + ", columnType=" + columnType);

      context.setInteger(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE, columnType);
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE, min);
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE, max);
    } finally {
      if(ps != null) {
        ps.close();
      }
      if(rs != null) {
        rs.close();
      }
    }
  }

  private void configureTableProperties(MutableContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    String dataSql;
    String fieldNames;

    String schemaName = fromJobConfig.fromJobConfig.schemaName;
    String tableName = fromJobConfig.fromJobConfig.tableName;
    String tableSql = fromJobConfig.fromJobConfig.sql;
    String tableColumns = fromJobConfig.fromJobConfig.columns;

    if (tableName != null && tableSql != null) {
      // when both fromTable name and fromTable sql are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

    } else if (tableName != null) {
      // when fromTable name is specified:

      // For databases that support schemas (IE: postgresql).
      String fullTableName = (schemaName == null) ? executor.delimitIdentifier(tableName) : executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

      if (tableColumns == null) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ");
        builder.append(fullTableName);
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
        builder.append(fullTableName);
        builder.append(" WHERE ");
        builder.append(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);
        dataSql = builder.toString();

        fieldNames = tableColumns;
      }
    } else if (tableSql != null) {
      // when fromTable sql is specified:

      assert tableSql.contains(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);

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
    } else {
      // when neither are specified:
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0008);
    }

    LOG.info("Using dataSql: " + dataSql);
    LOG.info("Field names: " + fieldNames);

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL, dataSql);
    context.setString(Constants.JOB_ETL_FIELD_NAMES, fieldNames);
  }
}

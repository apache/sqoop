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
import java.sql.Statement;
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

@edu.umd.cs.findbugs.annotations.SuppressWarnings({
        "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
public class GenericJdbcFromInitializer extends Initializer<LinkConfiguration, FromJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(GenericJdbcFromInitializer.class);

  private GenericJdbcExecutor executor;

  @Override
  public void initialize(InitializerContext context, LinkConfiguration linkConfig, FromJobConfiguration fromJobConfig) {
    executor = new GenericJdbcExecutor(linkConfig);

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
    executor = new GenericJdbcExecutor(linkConfig);

    String schemaName;
    if(fromJobConfig.fromJobConfig.tableName != null) {
      schemaName = executor.encloseIdentifiers(fromJobConfig.fromJobConfig.schemaName, fromJobConfig.fromJobConfig.tableName);
    } else {
      schemaName = "Query";
    }

    Schema schema = new Schema(schemaName);
    ResultSetMetaData rsmt = null;
    try (Statement statement = executor.getConnection().createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
         ResultSet rs = statement.executeQuery(context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL)
                 .replace(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0"));) {

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
      String [] primaryKeyColumns = executor.getPrimaryKey(jobConf.fromJobConfig.schemaName, jobConf.fromJobConfig.tableName);
      LOG.info("Found primary key columns [" + StringUtils.join(primaryKeyColumns, ", ") + "]");
      if(primaryKeyColumns.length == 0) {
        throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0025, "Please specify partition column.");
      } else if (primaryKeyColumns.length > 1) {
        LOG.warn("Table have compound primary key, for partitioner we're using only first column of the key: " + primaryKeyColumns[0]);
      }

      partitionColumnName = primaryKeyColumns[0];
    }
    // If we don't have partition column name, we will error out
    if (partitionColumnName != null) {
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNNAME, executor.encloseIdentifier(partitionColumnName));
    } else {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0005);
    }
    LOG.info("Using partition column: " + partitionColumnName);

    // From fragment for subsequent queries
    String fromFragment;
    if(tableImport) {
      fromFragment = executor.encloseIdentifiers(jobConf.fromJobConfig.schemaName, jobConf.fromJobConfig.tableName);
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
      sb.append("MAX(").append(executor.encloseIdentifier(jobConf.incrementalRead.checkColumn)).append(") ");
      sb.append("FROM ");
      sb.append(fromFragment);

      String incrementalNewMaxValueQuery = sb.toString();
      LOG.info("Incremental new max value query:  " + incrementalNewMaxValueQuery);

      try (Statement statement = executor.getConnection().createStatement(
              ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
           ResultSet rs = statement.executeQuery(incrementalNewMaxValueQuery);) {
        if (!rs.next()) {
          throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0022);
        }

        incrementalMaxValue = rs.getString(1);
        context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_LAST_INCREMENTAL_VALUE, incrementalMaxValue);
        LOG.info("New maximal value for incremental import is " + incrementalMaxValue);
      }
    }

    // Retrieving min and max values for partition column
    String minMaxQuery = jobConf.fromJobConfig.boundaryQuery;
    if (minMaxQuery == null) {
      sb.setLength(0);
      sb.append("SELECT ");
      sb.append("MIN(").append(executor.encloseIdentifier(partitionColumnName)).append("), ");
      sb.append("MAX(").append(executor.encloseIdentifier(partitionColumnName)).append(") ");
      sb.append("FROM ").append(fromFragment).append(" ");

      if(incrementalImport) {
        sb.append("WHERE ");
        sb.append(executor.encloseIdentifier(jobConf.incrementalRead.checkColumn)).append(" > ?");
        sb.append(" AND ");
        sb.append(executor.encloseIdentifier(jobConf.incrementalRead.checkColumn)).append(" <= ?");
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

    // Assertion that should be true based on our validations
    assert (tableName != null && tableSql == null) || (tableName == null && tableSql != null);

    if (tableName != null) {
      // For databases that support schemas (IE: postgresql).
      String fullTableName = executor.encloseIdentifiers(schemaName, tableName);

      if (tableColumns == null) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ");
        builder.append(fullTableName);
        builder.append(" WHERE ");
        builder.append(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);
        dataSql = builder.toString();

        String[] queryColumns = executor.getQueryColumns(dataSql.replace(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0"));
        fieldNames = executor.columnList(queryColumns);
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
    } else {
      assert tableSql.contains(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);

      if (tableColumns == null) {
        dataSql = tableSql;

        String[] queryColumns = executor.getQueryColumns(dataSql.replace(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN, "1 = 0"));
        fieldNames = executor.columnList(queryColumns);
      } else {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        builder.append(tableColumns);
        builder.append(" FROM ");
        builder.append("(");
        builder.append(tableSql);
        builder.append(") ");
        builder.append(GenericJdbcConnectorConstants.SUBQUERY_ALIAS);
        dataSql = builder.toString();

        fieldNames = tableColumns;
      }
    }

    LOG.info("Using dataSql: " + dataSql);
    LOG.info("Field names: " + fieldNames);

    context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_FROM_DATA_SQL, dataSql);
    context.setString(Constants.JOB_ETL_FIELD_NAMES, fieldNames);
  }
}

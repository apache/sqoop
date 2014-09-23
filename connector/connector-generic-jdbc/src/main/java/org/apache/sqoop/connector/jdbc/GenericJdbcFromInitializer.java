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
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.util.SqlTypesUtils;
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
  public void initialize(InitializerContext context, LinkConfiguration linkConf, FromJobConfiguration fromJobConf) {
    configureJdbcProperties(context.getContext(), linkConf, fromJobConf);
    try {
      configurePartitionProperties(context.getContext(), linkConf, fromJobConf);
      configureTableProperties(context.getContext(), linkConf, fromJobConf);
    } finally {
      executor.close();
    }
  }

  @Override
  public List<String> getJars(InitializerContext context, LinkConfiguration linkConf, FromJobConfiguration fromJobConf) {
    List<String> jars = new LinkedList<String>();

    jars.add(ClassUtils.jarForClass(linkConf.link.jdbcDriver));

    return jars;
  }

  @Override
  public Schema getSchema(InitializerContext context, LinkConfiguration linkConf, FromJobConfiguration fromJobConf) {
    configureJdbcProperties(context.getContext(), linkConf, fromJobConf);

    String schemaName = fromJobConf.fromJobConfig.tableName;
    if(schemaName == null) {
      schemaName = "Query";
    } else if(fromJobConf.fromJobConfig.schemaName != null) {
      schemaName = fromJobConf.fromJobConfig.schemaName + "." + schemaName;
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
      if (executor != null) {
        executor.close();
      }
    }
  }

  private void configureJdbcProperties(MutableContext context, LinkConfiguration connectionConfig, FromJobConfiguration fromJobConfig) {
    String driver = connectionConfig.link.jdbcDriver;
    String url = connectionConfig.link.connectionString;
    String username = connectionConfig.link.username;
    String password = connectionConfig.link.password;

    assert driver != null;
    assert url != null;

    executor = new GenericJdbcExecutor(driver, url, username, password);
  }

  private void configurePartitionProperties(MutableContext context, LinkConfiguration connectionConfig, FromJobConfiguration fromJobConfig) {
    // ----- configure column name -----

    String partitionColumnName = fromJobConfig.fromJobConfig.partitionColumn;

    if (partitionColumnName == null) {
      // if column is not specified by the user,
      // find the primary key of the fromTable (when there is a fromTable).
      String tableName = fromJobConfig.fromJobConfig.tableName;
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

    String minMaxQuery = fromJobConfig.fromJobConfig.boundaryQuery;

    if (minMaxQuery == null) {
      StringBuilder builder = new StringBuilder();

      String schemaName = fromJobConfig.fromJobConfig.schemaName;
      String tableName = fromJobConfig.fromJobConfig.tableName;
      String tableSql = fromJobConfig.fromJobConfig.sql;

      if (tableName != null && tableSql != null) {
        // when both fromTable name and fromTable sql are specified:
        throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0007);

      } else if (tableName != null) {
        // when fromTable name is specified:

        // For databases that support schemas (IE: postgresql).
        String fullTableName = (schemaName == null) ? executor.delimitIdentifier(tableName) : executor.delimitIdentifier(schemaName) + "." + executor.delimitIdentifier(tableName);

        String column = partitionColumnName;
        builder.append("SELECT MIN(");
        builder.append(column);
        builder.append("), MAX(");
        builder.append(column);
        builder.append(") FROM ");
        builder.append(fullTableName);

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

      int columnType = rsmd.getColumnType(1);
      String min = rs.getString(1);
      String max = rs.getString(2);

      LOG.info("Boundaries: min=" + min + ", max=" + max + ", columnType=" + columnType);

      context.setInteger(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_COLUMNTYPE, columnType);
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MINVALUE, min);
      context.setString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_PARTITION_MAXVALUE, max);

    } catch (SQLException e) {
      throw new SqoopException(
          GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0006, e);
    }
  }

  private void configureTableProperties(MutableContext context, LinkConfiguration connectionConfig, FromJobConfiguration fromJobConfig) {
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

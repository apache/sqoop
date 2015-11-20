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
package org.apache.sqoop.connector.jdbc.oracle;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTableColumn;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTableColumns;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.InsertMode;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.UpdateMode;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleVersion;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;
import org.apache.sqoop.schema.type.Column;
import org.joda.time.LocalDateTime;

public class OracleJdbcLoader extends Loader<LinkConfiguration, ToJobConfiguration> {

  private static final Logger LOG =
      Logger.getLogger(OracleJdbcToDestroyer.class);

  private long rowsWritten = 0;
  private LoaderContext context;
  private Connection connection;
  private OracleVersion oracleVersion;
  private OracleTable table; // <- If exporting into a partitioned
                               // table, this table will be unique for
                               // this mapper
  private OracleTableColumns tableColumns; // <- The columns in the
                                           // table we're inserting rows
                                           // into
  private int mapperId; // <- The index of this Hadoop mapper
  private boolean tableHasMapperRowNumberColumn; // <- Whether the export
                                                   // table contain the column
                                                   // SQOOP_MAPPER_ROW
  private long mapperRowNumber; // <- The 1-based row number being processed
                                  // by this mapper. It's inserted into the
                                  // "SQOOP_MAPPER_ROW" column
  private boolean useAppendValuesOracleHint = false; // <- Whether to use the
                                                     // " /*+APPEND_VALUES*/ " hint
                                                     // within the Oracle SQL
                                                     // statement we generate
  private long numberOfRowsSkipped; // <- The number of rows encountered
                                    // during configurePreparedStatement()
                                    // that had a NULL value for (one of) the
                                    // update columns. This row was therefore
                                    // skipped.
  private String[] updateColumnNames;
  private int rowsPerBatch;
  private int rowsPerCommit;


  private void setupInsert(LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) throws SQLException {
    // Is each mapper inserting rows into a unique table?...
    InsertMode insertMode = OracleUtilities.getExportInsertMode(
        jobConfiguration.toJobConfig, context.getContext());

    if(insertMode==InsertMode.ExchangePartition) {
      Object sysDateTime =
          OracleUtilities.recallOracleDateTime(context.getContext());
      table = OracleUtilities.generateExportTableMapperTableName(
          mapperId, sysDateTime, null);

    } else {
      table = OracleUtilities.decodeOracleTableName(
          linkConfiguration.connectionConfig.username,
          jobConfiguration.toJobConfig.tableName);
    }

    // Should we use the APPEND_VALUES Oracle hint?...
    useAppendValuesOracleHint = false;
    if (insertMode == InsertMode.ExchangePartition) {
      // NB: "Direct inserts" cannot utilize APPEND_VALUES, otherwise Oracle
      // will serialize
      // the N mappers, causing a lot of lock contention.
      useAppendValuesOracleHint = canUseOracleAppendValuesHint();
    }
  }

  private void setupUpdate(LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) throws SQLException {
    UpdateMode updateMode = OracleUtilities.getExportUpdateMode(
        jobConfiguration.toJobConfig);

    Object sysDateTime =
        OracleUtilities.recallOracleDateTime(context.getContext());
    table = OracleUtilities.generateExportTableMapperTableName(
        mapperId, sysDateTime, null);

    updateColumnNames = OracleUtilities.
        getExportUpdateKeyColumnNames(jobConfiguration.toJobConfig);

    if (updateMode == UpdateMode.Merge || updateMode == UpdateMode.Update) {
      // Should we use the APPEND_VALUES Oracle hint?...
      useAppendValuesOracleHint = canUseOracleAppendValuesHint();
    }

  }

  @Override
  public void load(LoaderContext context, LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) throws Exception {
    LOG.debug("Running Oracle JDBC connector loader");
    this.context = context;

    //TODO: Mapper ID
    mapperId = 1;
    //TODO: Hardcoded values
    rowsPerBatch = 5000;
    rowsPerCommit = 5000;

    // Retrieve the JDBC URL that should be used by this mapper.
    // We achieve this by modifying the JDBC URL property in the
    // configuration, prior to the OraOopDBRecordWriter's (ancestral)
    // constructor using the configuration to establish a connection
    // to the database - via DBConfiguration.getConnection()...
    String mapperJdbcUrlPropertyName =
        OracleUtilities.getMapperJdbcUrlPropertyName(mapperId);

    // Get this mapper's JDBC URL
    String mapperJdbcUrl = context.getString(mapperJdbcUrlPropertyName, null);

    LOG.debug(String.format("Mapper %d has a JDBC URL of: %s", mapperId,
        mapperJdbcUrl == null ? "<null>" : mapperJdbcUrl));

    connection = OracleConnectionFactory.createOracleJdbcConnection(
        OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
        mapperJdbcUrl,
        linkConfiguration.connectionConfig.username,
        linkConfiguration.connectionConfig.password);
    String thisOracleInstanceName =
        OracleQueries.getCurrentOracleInstanceName(connection);
    LOG.info(String.format(
        "This record writer is connected to Oracle via the JDBC URL: \n"
            + "\t\"%s\"\n" + "\tto the Oracle instance: \"%s\"", connection
            .toString(), thisOracleInstanceName));
    OracleConnectionFactory.initializeOracleConnection(
        connection, linkConfiguration.connectionConfig);
    connection.setAutoCommit(false);
    oracleVersion = OracleQueries.getOracleVersion(connection);

    if (jobConfiguration.toJobConfig.updateKey == null ||
        jobConfiguration.toJobConfig.updateKey.isEmpty()) {
      setupInsert(linkConfiguration, jobConfiguration);
    } else {
      setupUpdate(linkConfiguration, jobConfiguration);
    }

    tableColumns = OracleQueries.getToTableColumns(
        connection, table, true, false);

    tableHasMapperRowNumberColumn =
        tableColumns.findColumnByName(
            OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_MAPPER_ROW) != null;

    // Has the user forced the use of APPEND_VALUES either on or off?...
    useAppendValuesOracleHint =
        allowUserToOverrideUseOfTheOracleAppendValuesHint(
            jobConfiguration.toJobConfig,
            useAppendValuesOracleHint);

    insertData();
    connection.close();
  }

  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }

  private void insertData() throws Exception {
    // If using APPEND_VALUES, check the batch size and commit frequency...
    if (useAppendValuesOracleHint) {
      if(rowsPerBatch < OracleJdbcConnectorConstants.
          MIN_APPEND_VALUES_BATCH_SIZE_DEFAULT) {
        LOG.info(String
            .format(
                "The number of rows per batch-insert has been changed from %d "
                    + "to %d. This is in response "
                    + "to the Oracle APPEND_VALUES hint being used.",
                    rowsPerBatch, OracleJdbcConnectorConstants.
                    MIN_APPEND_VALUES_BATCH_SIZE_DEFAULT));
        rowsPerBatch = OracleJdbcConnectorConstants.
            MIN_APPEND_VALUES_BATCH_SIZE_DEFAULT;
      }
      // Need to commit after each batch when using APPEND_VALUES
      if(rowsPerCommit!=rowsPerBatch) {
        LOG.info(String
            .format(
                "The number of rows to insert per commit has been "
                    + "changed from %d to %d. This is in response "
                    + "to the Oracle APPEND_VALUES hint being used.",
                    rowsPerCommit, rowsPerBatch));
        rowsPerCommit = rowsPerBatch;
      }
    }

    mapperRowNumber = 1;

    String sql = getBatchInsertSqlStatement(useAppendValuesOracleHint
        ? "/*+APPEND_VALUES*/" : "");
    PreparedStatement statement = connection.prepareStatement(sql);

    Column[] columns = context.getSchema().getColumnsArray();
    Object[] array;
    boolean checkUpdateColumns = false;
    List<Integer> updateColumnIndexes = null;
    if(updateColumnNames!=null) {
      checkUpdateColumns = true;
      updateColumnIndexes = new ArrayList<Integer>();
      for (int idx = 0; idx < this.updateColumnNames.length; idx++) {
        for (int i = 0; i < columns.length; i++) {
          if(columns[i].getName().equals(updateColumnNames[idx])) {
            updateColumnIndexes.add(i);
          }
        }
      }
    }

    while ((array = context.getDataReader().readArrayRecord()) != null) {
      if(checkUpdateColumns) {
        boolean updateKeyValueIsNull = false;
        for (Integer i : updateColumnIndexes) {
          Object updateKeyValue = array[i];
          if (updateKeyValue == null) {
            this.numberOfRowsSkipped++;
            updateKeyValueIsNull = true;
            break;
          }
        }

        if (updateKeyValueIsNull) {
          continue;
        }
      }
      rowsWritten++;
      configurePreparedStatementColumns(statement, columns, array);
      if(rowsWritten % rowsPerBatch == 0) {
        statement.executeBatch();
      }
      if(rowsWritten % rowsPerCommit == 0) {
        connection.commit();
      }
    }
    if(rowsWritten % rowsPerBatch != 0) {
      statement.executeBatch();
    }
    connection.commit();
    statement.close();

    if (numberOfRowsSkipped > 0) {
      LOG.warn(String.format(
          "%d records were skipped due to a NULL value within one of the "
        + "update-key column(s).\nHaving a NULL value prevents a record "
        + "from being able to be matched to a row in the Oracle table.",
              numberOfRowsSkipped));
    }
  }

  private String getBatchInsertSqlStatement(String oracleHint) {

    // String[] columnNames = this.getColumnNames();
    StringBuilder sqlNames = new StringBuilder();
    StringBuilder sqlValues = new StringBuilder();

    /*
     * NOTE: "this.oracleTableColumns" may contain a different list of columns
     * than "this.getColumnNames()". This is because: (1)
     * "this.getColumnNames()" includes columns with data-types that are not
     * supported by OraOop. (2) "this.oracleTableColumns" includes any
     * pseudo-columns that we've added to the export table (and don't exist in
     * the HDFS file being read). For example, if exporting to a partitioned
     * table (that OraOop created), there are two pseudo-columns we added to
     * the table to identify the export job and the mapper.
     */
    List<String> columnNamesList = new ArrayList<String>();
    for(Column column : context.getSchema().getColumnsList()) {
      columnNamesList.add(column.getName());
    }

    int colCount = 0;
    for (int idx = 0; idx < this.tableColumns.size(); idx++) {
      OracleTableColumn oracleTableColumn = this.tableColumns.get(idx);
      String columnName = oracleTableColumn.getName();
      if(columnNamesList.contains(columnName) ||
          OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_PARTITION
              .equals(columnName) ||
          OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_SUBPARTITION
              .equals(columnName) ||
          OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_MAPPER_ROW
          .equals(columnName)) {
        // column names...
        if (colCount > 0) {
          sqlNames.append("\n,");
        }
        sqlNames.append(columnName);

        // column values...
        if (colCount > 0) {
          sqlValues.append("\n,");
        }

        String pseudoColumnValue =
            generateInsertValueForPseudoColumn(columnName);

        String bindVarName = null;

        if (pseudoColumnValue != null) {
          bindVarName = pseudoColumnValue;
        } else if (oracleTableColumn.getOracleType() == OracleQueries
            .getOracleType("STRUCT")) {
          if (oracleTableColumn.getDataType().equals(
              OracleJdbcConnectorConstants.Oracle.URITYPE)) {
            bindVarName =
                String.format("urifactory.getUri(%s)",
                    columnNameToBindVariable(columnName));
          }
          //TODO: Date as string?
        /*} else if (getConf().getBoolean(
            OraOopConstants.ORAOOP_MAP_TIMESTAMP_AS_STRING,
            OraOopConstants.ORAOOP_MAP_TIMESTAMP_AS_STRING_DEFAULT)) {
          if (oracleTableColumn.getOracleType() == OraOopOracleQueries
              .getOracleType("DATE")) {
            bindVarName =
                String.format("to_date(%s, 'yyyy-mm-dd hh24:mi:ss')",
                    columnNameToBindVariable(columnName));
          } else if (oracleTableColumn.getOracleType() == OraOopOracleQueries
              .getOracleType("TIMESTAMP")) {
            bindVarName =
                String.format("to_timestamp(%s, 'yyyy-mm-dd hh24:mi:ss.ff')",
                    columnNameToBindVariable(columnName));
          } else if (oracleTableColumn.getOracleType() == OraOopOracleQueries
              .getOracleType("TIMESTAMPTZ")) {
            bindVarName =
                String.format(
                    "to_timestamp_tz(%s, 'yyyy-mm-dd hh24:mi:ss.ff TZR')",
                    columnNameToBindVariable(columnName));
          } else if (oracleTableColumn.getOracleType() == OraOopOracleQueries
              .getOracleType("TIMESTAMPLTZ")) {
            bindVarName =
                String.format(
                    "to_timestamp_tz(%s, 'yyyy-mm-dd hh24:mi:ss.ff TZR')",
                    columnNameToBindVariable(columnName));
          }*/
        }

        if (bindVarName == null) {
          bindVarName = columnNameToBindVariable(columnName);
        }

        sqlValues.append(bindVarName);

        colCount++;
      }
    }

    String sql =
        String.format("insert %s into %s\n" + "(%s)\n" + "values\n"
            + "(%s)\n", oracleHint, this.table.toString(), sqlNames
            .toString(), sqlValues.toString());

    LOG.info("Batch-Mode insert statement:\n" + sql);
    return sql;
  }

  private String generateInsertValueForPseudoColumn(String columnName) {

    if (columnName.equalsIgnoreCase(
        OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_PARTITION)) {

      String partitionValueStr =
          context.getString(
              OracleJdbcConnectorConstants.ORAOOP_EXPORT_PARTITION_DATE_VALUE);
      if (partitionValueStr == null) {
        throw new RuntimeException(
            "Unable to recall the value of the partition date-time.");
      }

      return String.format("to_date('%s', '%s')", partitionValueStr,
          OracleJdbcConnectorConstants.ORAOOP_EXPORT_PARTITION_DATE_FORMAT);
    }

    if (columnName.equalsIgnoreCase(
        OracleJdbcConnectorConstants.COLUMN_NAME_EXPORT_SUBPARTITION)) {
      return Integer.toString(this.mapperId);
    }

    return null;
  }

  private String columnNameToBindVariable(String columnName) {
    return ":" + columnName;
  }

  private void configurePreparedStatementColumns(
      PreparedStatement statement, Column[] columns, Object[] array)
      throws SQLException {

    String bindValueName;

    if (this.tableHasMapperRowNumberColumn) {
      bindValueName = columnNameToBindVariable(OracleJdbcConnectorConstants.
          COLUMN_NAME_EXPORT_MAPPER_ROW).replaceFirst(":", "");
      try {
        OracleQueries.setLongAtName(statement, bindValueName,
            this.mapperRowNumber);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      this.mapperRowNumber++;
    }

    for (int i = 0; i < array.length; i++) {
      String colName = columns[i].getName();
      bindValueName = columnNameToBindVariable(colName).replaceFirst(":", "");
      OracleTableColumn oracleTableColumn =
          tableColumns.findColumnByName(colName);
      setBindValueAtName(statement, bindValueName, array[i],
          oracleTableColumn);
    }
    statement.addBatch();
  }

  private void setBindValueAtName(PreparedStatement statement,
      String bindValueName, Object bindValue, OracleTableColumn column)
      throws SQLException {
    if (column.getOracleType()
        == OracleQueries.getOracleType("NUMBER")) {
      OracleQueries.setBigDecimalAtName(statement, bindValueName,
          (BigDecimal) bindValue);
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("VARCHAR")) {
      OracleQueries.setStringAtName(statement, bindValueName,
          (String) bindValue);
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("TIMESTAMP")
        || column.getOracleType() == OracleQueries
            .getOracleType("TIMESTAMPTZ")
        || column.getOracleType() == OracleQueries
            .getOracleType("TIMESTAMPLTZ")) {
      Object objValue = bindValue;
      if (objValue instanceof LocalDateTime) {
        //TODO: Improve date handling
        LocalDateTime value = (LocalDateTime) objValue;
        Timestamp timestampValue =
            new Timestamp(value.toDateTime().getMillis());
        OracleQueries.setTimestampAtName(statement, bindValueName,
            timestampValue);
      } else {
        String value = (String) objValue;

        if (value == null || value.equalsIgnoreCase("null")) {
          value = "";
        }

        OracleQueries.setStringAtName(statement, bindValueName, value);
      }
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("BINARY_DOUBLE")) {
      Double value = (Double) bindValue;
      if (value != null) {
        OracleQueries.setBinaryDoubleAtName(statement, bindValueName,
            value);
      } else {
        OracleQueries.setObjectAtName(statement, bindValueName, null);
      }
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("BINARY_FLOAT")) {
      Float value = (Float) bindValue;
      if (value != null) {
        OracleQueries.setBinaryFloatAtName(statement, bindValueName,
            value);
      } else {
        OracleQueries.setObjectAtName(statement, bindValueName, null);
      }
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("STRUCT")) { // <- E.g. URITYPE
      if (column.getDataType().equals(
          OracleJdbcConnectorConstants.Oracle.URITYPE)) {
        String value = (String) bindValue;
        OracleQueries.setStringAtName(statement, bindValueName, value);
      } else {
        String msg =
            String.format(
                "%s needs to be updated to cope with the data-type: %s "
                    + "where the Oracle data_type is \"%s\".",
                OracleUtilities.getCurrentMethodName(), column.getDataType(),
                column.getOracleType());
        LOG.error(msg);
        throw new UnsupportedOperationException(msg);
      }
    } else {
      // LOB data-types are currently not supported during
      // a Sqoop Export.
      // JIRA: SQOOP-117
      // OraOopConstants.SUPPORTED_EXPORT_ORACLE_DATA_TYPES_CLAUSE
      // will already have excluded all LOB columns.

      // case oracle.jdbc.OracleTypes.CLOB:
      // {
      // oracle.sql.CLOB clob = new
      // oracle.sql.CLOB(connection);
      // Object value = fieldMap.get(colName);
      // //clob.set
      // statement.setCLOBAtName(bindValueName, clob);
      // break;
      // }
      String msg =
          String.format(
              "%s may need to be updated to cope with the data-type: %s",
              OracleUtilities.getCurrentMethodName(), column.getOracleType());
      LOG.debug(msg);

      OracleQueries
          .setObjectAtName(statement, bindValueName, bindValue);
    }
  }

  private boolean canUseOracleAppendValuesHint() {

    // Should we use the APPEND_VALUES Oracle hint?...
    // (Yes, if this is Oracle 11.2 or above)...
    boolean result = oracleVersion.isGreaterThanOrEqualTo(11, 2, 0, 0);

    // If there is a BINARY_DOUBLE or BINARY_FLOAT column, then we'll avoid
    // using
    // the APPEND_VALUES hint. If there is a NULL in the HDFS file, then we'll
    // encounter
    // "ORA-12838: cannot read/modify an object after modifying it in parallel"
    // due to the JDBC driver issuing the INSERT statement twice to the database
    // without a COMMIT in between (as was observed via WireShark).
    // We're not sure why this happens - we just know how to avoid it.
    if (result) {
      boolean binaryDoubleColumnExists = false;
      boolean binaryFloatColumnExists = false;
      for (int idx = 0; idx < this.tableColumns.size(); idx++) {
        OracleTableColumn oracleTableColumn = this.tableColumns.get(idx);
        if(oracleTableColumn.getOracleType()==
            OracleQueries.getOracleType("BINARY_DOUBLE")) {
          binaryDoubleColumnExists = true;
        }
        if(oracleTableColumn.getOracleType()==
            OracleQueries.getOracleType("BINARY_FLOAT")) {
          binaryFloatColumnExists = true;
        }
      }

      if (binaryDoubleColumnExists || binaryFloatColumnExists) {
        result = false;
        LOG.info("The APPEND_VALUES Oracle hint will not be used for the "
            + "INSERT SQL statement, as the Oracle table "
            + "contains either a BINARY_DOUBLE or BINARY_FLOAT column.");
      }
    }

    return result;
  }

  protected boolean allowUserToOverrideUseOfTheOracleAppendValuesHint(
      ToJobConfig jobConfig, boolean useAppendValuesOracleHint) {

    boolean result = useAppendValuesOracleHint;

    // Has the user forced the use of APPEND_VALUES either on or off?...
    switch (OracleUtilities.getOracleAppendValuesHintUsage(jobConfig)) {

      case OFF:
        result = false;
        LOG.debug(String
            .format(
                "Use of the APPEND_VALUES Oracle hint has been forced OFF. "
                + "(It was %s to used).",
                useAppendValuesOracleHint ? "going" : "not going"));
        break;

      case ON:
        result = true;
        LOG.debug(String
            .format(
                "Use of the APPEND_VALUES Oracle hint has been forced ON. "
                + "(It was %s to used).",
                useAppendValuesOracleHint ? "going" : "not going"));
        break;

      case AUTO:
        LOG.debug(String.format("The APPEND_VALUES Oracle hint %s be used.",
            result ? "will" : "will not"));
        break;

      default:
        throw new RuntimeException("Invalid value for APPEND_VALUES.");
    }
    return result;
  }
}

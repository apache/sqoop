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

package org.apache.sqoop.manager.oracle;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AsyncSqlOutputFormat;
import com.cloudera.sqoop.mapreduce.ExportOutputFormat;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

abstract class OraOopOutputFormatBase<K extends SqoopRecord, V> extends
    ExportOutputFormat<K, V> {

  private static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopOutputFormatBase.class);

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {

    super.checkOutputSpecs(context);

    Configuration conf = context.getConfiguration();

    // This code is now running on a Datanode in the Hadoop cluster, so we
    // need to enable debug logging in this JVM...
    OraOopUtilities.enableDebugLoggingIfRequired(conf);
  }

  protected int getMapperId(TaskAttemptContext context) {

    return context.getTaskAttemptID().getTaskID().getId();
  }

  protected void applyMapperJdbcUrl(TaskAttemptContext context, int mapperId) {

    Configuration conf = context.getConfiguration();

    // Retrieve the JDBC URL that should be used by this mapper.
    // We achieve this by modifying the JDBC URL property in the
    // configuration, prior to the OraOopDBRecordWriter's (ancestral)
    // constructor using the configuration to establish a connection
    // to the database - via DBConfiguration.getConnection()...
    String mapperJdbcUrlPropertyName =
        OraOopUtilities.getMapperJdbcUrlPropertyName(mapperId, conf);

    // Get this mapper's JDBC URL
    String mapperJdbcUrl = conf.get(mapperJdbcUrlPropertyName, null);

    LOG.debug(String.format("Mapper %d has a JDBC URL of: %s", mapperId,
        mapperJdbcUrl == null ? "<null>" : mapperJdbcUrl));

    if (mapperJdbcUrl != null) {
      conf.set(DBConfiguration.URL_PROPERTY, mapperJdbcUrl);
    }
  }

  protected boolean canUseOracleAppendValuesHint(TaskAttemptContext context) {

    Configuration conf = context.getConfiguration();

    // Should we use the APPEND_VALUES Oracle hint?...
    // (Yes, if this is Oracle 11.2 or above)...
    OracleVersion oracleVersion =
        new OracleVersion(conf.getInt(
            OraOopConstants.ORAOOP_ORACLE_DATABASE_VERSION_MAJOR, 0), conf
            .getInt(OraOopConstants.ORAOOP_ORACLE_DATABASE_VERSION_MINOR, 0),
            0, 0, "");

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
      boolean binaryDoubleColumnExists =
          conf.getBoolean(OraOopConstants.TABLE_CONTAINS_BINARY_DOUBLE_COLUMN,
              false);
      boolean binaryFloatColumnExists =
          conf.getBoolean(OraOopConstants.TABLE_CONTAINS_BINARY_FLOAT_COLUMN,
              false);
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
      TaskAttemptContext context, boolean useAppendValuesOracleHint) {

    Configuration conf = context.getConfiguration();

    boolean result = useAppendValuesOracleHint;

    // Has the user forced the use of APPEND_VALUES either on or off?...
    switch (OraOopUtilities.getOracleAppendValuesHintUsage(conf)) {

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

  protected void updateBatchSizeInConfigurationToAllowOracleAppendValuesHint(
      TaskAttemptContext context) {

    Configuration conf = context.getConfiguration();

    // If using APPEND_VALUES, check the batch size and commit frequency...
    int originalBatchesPerCommit =
        conf.getInt(AsyncSqlOutputFormat.STATEMENTS_PER_TRANSACTION_KEY, 0);
    if (originalBatchesPerCommit != 1) {
      conf.setInt(AsyncSqlOutputFormat.STATEMENTS_PER_TRANSACTION_KEY, 1);
      LOG.info(String
          .format(
              "The number of batch-inserts to perform per commit has been "
                  + "changed from %d to %d. This is in response "
                  + "to the Oracle APPEND_VALUES hint being used.",
              originalBatchesPerCommit, 1));
    }

    int originalBatchSize =
        conf.getInt(AsyncSqlOutputFormat.RECORDS_PER_STATEMENT_KEY, 0);
    int minAppendValuesBatchSize =
        OraOopUtilities.getMinAppendValuesBatchSize(conf);
    if (originalBatchSize < minAppendValuesBatchSize) {
      conf.setInt(AsyncSqlOutputFormat.RECORDS_PER_STATEMENT_KEY,
          minAppendValuesBatchSize);
      LOG.info(String
          .format(
              "The number of rows per batch-insert has been changed from %d "
                  + "to %d. This is in response "
                  + "to the Oracle APPEND_VALUES hint being used.",
              originalBatchSize, minAppendValuesBatchSize));
    }
  }

  abstract class OraOopDBRecordWriterBase extends
      ExportOutputFormat<K, V>.ExportRecordWriter<K, V> {

    protected OracleTable oracleTable; // <- If exporting into a partitioned
                                       // table, this table will be unique for
                                       // this mapper
    private OracleTableColumns oracleTableColumns; // <- The columns in the
                                                   // table we're inserting rows
                                                   // into
    protected int mapperId; // <- The index of this Hadoop mapper
    protected boolean tableHasMapperRowNumberColumn; // <- Whether the export
                                                     // table contain the column
                                                     // ORAOOP_MAPPER_ROW
    protected long mapperRowNumber; // <- The 1-based row number being processed
                                    // by this mapper. It's inserted into the
                                    // "ORAOOP_MAPPER_ROW" column

    public OraOopDBRecordWriterBase(TaskAttemptContext context, int mapperId)
        throws ClassNotFoundException, SQLException {

      super(context);
      this.mapperId = mapperId;
      this.mapperRowNumber = 1;

      Configuration conf = context.getConfiguration();

      // Log any info that might be useful to us...
      logBatchSettings();

      // Connect to Oracle...
      Connection connection = this.getConnection();

      String thisOracleInstanceName =
          OraOopOracleQueries.getCurrentOracleInstanceName(connection);
      LOG.info(String.format(
          "This record writer is connected to Oracle via the JDBC URL: \n"
              + "\t\"%s\"\n" + "\tto the Oracle instance: \"%s\"", connection
              .toString(), thisOracleInstanceName));

      // Initialize the Oracle session...
      OracleConnectionFactory.initializeOracleConnection(connection, conf);
      connection.setAutoCommit(false);
    }

    protected void setOracleTableColumns(
        OracleTableColumns newOracleTableColumns) {

      this.oracleTableColumns = newOracleTableColumns;
      this.tableHasMapperRowNumberColumn =
          this.oracleTableColumns.findColumnByName(
              OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW) != null;
    }

    protected OracleTableColumns getOracleTableColumns() {

      return this.oracleTableColumns;
    }

    protected void getExportTableAndColumns(TaskAttemptContext context)
        throws SQLException {

      Configuration conf = context.getConfiguration();

      String schema =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_OWNER);
      String localTableName =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_NAME);

      if (schema == null || schema.isEmpty() || localTableName == null
          || localTableName.isEmpty()) {
        throw new RuntimeException(
            "Unable to recall the schema and name of the Oracle table "
            + "being exported.");
      }

      this.oracleTable = new OracleTable(schema, localTableName);

      setOracleTableColumns(OraOopOracleQueries.getTableColumns(this
          .getConnection(), this.oracleTable, OraOopUtilities
          .omitLobAndLongColumnsDuringImport(conf), OraOopUtilities
          .recallSqoopJobType(conf), true // <- onlyOraOopSupportedTypes
          , false // <- omitOraOopPseudoColumns
          ));
    }

    @Override
    protected PreparedStatement getPreparedStatement(
        List<SqoopRecord> userRecords) throws SQLException {

      Connection connection = this.getConnection();

      String sql = getBatchSqlStatement();
      LOG.debug(String.format("Prepared Statement SQL:\n%s", sql));

      PreparedStatement statement;

      try {
        // Synchronize on connection to ensure this does not conflict
        // with the operations in the update thread.
        synchronized (connection) {
          statement = connection.prepareStatement(sql);
        }

        configurePreparedStatement(statement, userRecords);
      } catch (Exception ex) {
        if (ex instanceof SQLException) {
          throw (SQLException) ex;
        } else {
          LOG.error(String.format("The following error occurred during %s",
              OraOopUtilities.getCurrentMethodName()), ex);
          throw new SQLException(ex);
        }
      }

      return statement;
    }

    @Override
    protected boolean isBatchExec() {

      return true;
    }

    @Override
    protected String getInsertStatement(int numRows) {

      throw new UnsupportedOperationException(String.format(
          "%s should not be called, as %s operates in batch mode.",
          OraOopUtilities.getCurrentMethodName(), this.getClass().getName()));
    }

    protected String getBatchInsertSqlStatement(String oracleHint) {

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

      int colCount = 0;
      for (int idx = 0; idx < this.oracleTableColumns.size(); idx++) {
        OracleTableColumn oracleTableColumn = this.oracleTableColumns.get(idx);
        String columnName = oracleTableColumn.getName();

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
        } else if (oracleTableColumn.getOracleType() == OraOopOracleQueries
            .getOracleType("STRUCT")) {
          if (oracleTableColumn.getDataType().equals(
              OraOopConstants.Oracle.URITYPE)) {
            bindVarName =
                String.format("urifactory.getUri(%s)",
                    columnNameToBindVariable(columnName));
          }
        } else if (getConf().getBoolean(
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
          }
        }

        if (bindVarName == null) {
          bindVarName = columnNameToBindVariable(columnName);
        }

        sqlValues.append(bindVarName);

        colCount++;
      }

      String sql =
          String.format("insert %s into %s\n" + "(%s)\n" + "values\n"
              + "(%s)\n", oracleHint, this.oracleTable.toString(), sqlNames
              .toString(), sqlValues.toString());

      LOG.info("Batch-Mode insert statement:\n" + sql);
      return sql;
    }

    abstract void configurePreparedStatement(
        PreparedStatement preparedStatement, List<SqoopRecord> userRecords)
        throws SQLException;

    private void setBindValueAtName(PreparedStatement statement,
        String bindValueName, Object bindValue, OracleTableColumn column)
        throws SQLException {
      if (column.getOracleType()
          == OraOopOracleQueries.getOracleType("NUMBER")) {
        OraOopOracleQueries.setBigDecimalAtName(statement, bindValueName,
            (BigDecimal) bindValue);
      } else if (column.getOracleType() == OraOopOracleQueries
          .getOracleType("VARCHAR")) {
        OraOopOracleQueries.setStringAtName(statement, bindValueName,
            (String) bindValue);
      } else if (column.getOracleType() == OraOopOracleQueries
          .getOracleType("TIMESTAMP")
          || column.getOracleType() == OraOopOracleQueries
              .getOracleType("TIMESTAMPTZ")
          || column.getOracleType() == OraOopOracleQueries
              .getOracleType("TIMESTAMPLTZ")) {
        Object objValue = bindValue;
        if (objValue instanceof Timestamp) {
          Timestamp value = (Timestamp) objValue;
          OraOopOracleQueries.setTimestampAtName(statement, bindValueName,
              value);
        } else {
          String value = (String) objValue;

          if (value == null || value.equalsIgnoreCase("null")) {
            value = "";
          }

          OraOopOracleQueries.setStringAtName(statement, bindValueName, value);
        }
      } else if (column.getOracleType() == OraOopOracleQueries
          .getOracleType("BINARY_DOUBLE")) {
        Double value = (Double) bindValue;
        if (value != null) {
          OraOopOracleQueries.setBinaryDoubleAtName(statement, bindValueName,
              value);
        } else {
          OraOopOracleQueries.setObjectAtName(statement, bindValueName, null);
        }
      } else if (column.getOracleType() == OraOopOracleQueries
          .getOracleType("BINARY_FLOAT")) {
        Float value = (Float) bindValue;
        if (value != null) {
          OraOopOracleQueries.setBinaryFloatAtName(statement, bindValueName,
              value);
        } else {
          OraOopOracleQueries.setObjectAtName(statement, bindValueName, null);
        }
      } else if (column.getOracleType() == OraOopOracleQueries
          .getOracleType("STRUCT")) { // <- E.g. URITYPE
        if (column.getDataType().equals(OraOopConstants.Oracle.URITYPE)) {
          String value = (String) bindValue;
          OraOopOracleQueries.setStringAtName(statement, bindValueName, value);
        } else {
          String msg =
              String.format(
                  "%s needs to be updated to cope with the data-type: %s "
                      + "where the Oracle data_type is \"%s\".",
                  OraOopUtilities.getCurrentMethodName(), column.getDataType(),
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
                OraOopUtilities.getCurrentMethodName(), column.getOracleType());
        LOG.debug(msg);

        OraOopOracleQueries
            .setObjectAtName(statement, bindValueName, bindValue);
      }
    }

    protected void configurePreparedStatementColumns(
        PreparedStatement statement, Map<String, Object> fieldMap)
        throws SQLException {

      String bindValueName;

      if (this.tableHasMapperRowNumberColumn) {
        bindValueName =
            columnNameToBindVariable(
                OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW).replaceFirst(
                ":", "");
        try {
          OraOopOracleQueries.setLongAtName(statement, bindValueName,
              this.mapperRowNumber);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        this.mapperRowNumber++;
      }

      Iterator<String> columnNameIterator = fieldMap.keySet().iterator();
      while (columnNameIterator.hasNext()) {
        String colName = columnNameIterator.next();
        bindValueName = columnNameToBindVariable(colName).replaceFirst(":", "");

        OracleTableColumn oracleTableColumn =
            oracleTableColumns.findColumnByName(colName);
        setBindValueAtName(statement, bindValueName, fieldMap.get(colName),
            oracleTableColumn);
      }
      statement.addBatch();
    }

    abstract String getBatchSqlStatement();

    protected String columnNameToBindVariable(String columnName) {

      return ":" + columnName;
    }

    @Override
    public void write(K key, V value) throws InterruptedException, IOException {

      try {
        super.write(key, value);
      } catch (IOException ex) {
        // This IOException may contain a SQLException that occurred
        // during the batch insert...
        showSqlBatchErrorDetails(ex);
        throw ex;
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {

      try {
        super.close(context);
      } catch (IOException ex) {
        // This IOException may contain a SQLException that occurred
        // during the batch insert...
        showSqlBatchErrorDetails(ex);
        throw ex;
      }
    }

    private void showSqlBatchErrorDetails(Exception exception) {

      if (OraOopUtilities.oracleSessionHasBeenKilled(exception)) {
        LOG.info("\n*********************************************************"
            + "\nThe Oracle session in use has been killed by a 3rd party."
            + "\n*********************************************************");
      }

      /*
       * Unfortunately, BatchUpdateException.getUpdateCounts() only returns
       * information about UPDATE statements (not INSERT) statements. Since
       * we're only performing INSERT statements, there's no extra information
       * we can provide to the user at this point.
       */

      // if(exception == null)
      // return;
      //
      // if(exception instanceof BatchUpdateException) {
      // BatchUpdateException ex = (BatchUpdateException)exception;
      //
      // int[] updateCounts = ex.getUpdateCounts();
      // LOG.error("The number of successful updates was: " +
      // updateCounts.length);
      //
      // // Recurse for chained exceptions...
      // SQLException nextEx = ex.getNextException();
      // while(nextEx != null) {
      // showSqlBatchErrorDetails(nextEx);
      // nextEx = nextEx.getNextException();
      // }
      // }
      //
      // // Recurse for nested exceptions...
      // Throwable cause = exception.getCause();
      // if(cause instanceof Exception)
      // showSqlBatchErrorDetails((Exception)cause);

    }

    protected Object getJobSysDate(TaskAttemptContext context) {

      Configuration conf = context.getConfiguration();
      return OraOopUtilities.recallOracleDateTime(conf,
          OraOopConstants.ORAOOP_JOB_SYSDATE);
    }

    protected OracleTable createUniqueMapperTable(TaskAttemptContext context)
        throws SQLException {

      // This mapper inserts data into a unique table before either:
      // - exchanging it into a subpartition of the 'real' export table; or
      // - merging it into the 'real' export table.

      Configuration conf = context.getConfiguration();

      Object sysDateTime = getJobSysDate(context);

      String schema = conf.get(OraOopConstants.ORAOOP_TABLE_OWNER);
      String localTableName = conf.get(OraOopConstants.ORAOOP_TABLE_NAME);

      OracleTable templateTable = new OracleTable(schema, localTableName);

      OracleTable mapperTable =
          OraOopUtilities.generateExportTableMapperTableName(this.mapperId,
              sysDateTime, null);

      // If this mapper is being reattempted in response to a failure, we need
      // to delete the
      // temporary table created by the previous attempt...
      OraOopOracleQueries.dropTable(this.getConnection(), mapperTable);

      String temporaryTableStorageClause =
          OraOopUtilities.getTemporaryTableStorageClause(conf);

      OraOopOracleQueries.createExportTableForMapper(this.getConnection(),
          mapperTable, temporaryTableStorageClause, templateTable
          , false); // <- addOraOopPartitionColumns

      LOG.debug(String.format("Created temporary mapper table %s", mapperTable
          .toString()));

      return mapperTable;
    }

    protected String generateInsertValueForPseudoColumn(String columnName) {

      if (columnName
          .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_PARTITION)) {

        String partitionValueStr =
            this.getConf().get(
                OraOopConstants.ORAOOP_EXPORT_PARTITION_DATE_VALUE, null);
        if (partitionValueStr == null) {
          throw new RuntimeException(
              "Unable to recall the value of the partition date-time.");
        }

        return String.format("to_date('%s', '%s')", partitionValueStr,
            OraOopConstants.ORAOOP_EXPORT_PARTITION_DATE_FORMAT);
      }

      if (columnName
          .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION)) {
        return Integer.toString(this.mapperId);
      }

      return null;
    }

    protected void logBatchSettings() {

      LOG.info(String.format("The number of rows per batch is: %d",
          this.rowsPerStmt));

      int stmtsPerTx =
          this.getConf().getInt(
              AsyncSqlOutputFormat.STATEMENTS_PER_TRANSACTION_KEY,
              AsyncSqlOutputFormat.DEFAULT_STATEMENTS_PER_TRANSACTION);

      LOG.info(String.format("The number of batches per commit is: %d",
          stmtsPerTx));
    }

  }

}

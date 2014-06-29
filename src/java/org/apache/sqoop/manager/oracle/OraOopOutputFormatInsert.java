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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Insert into an Oracle table based on emitted keys.
 */
public class OraOopOutputFormatInsert<K extends SqoopRecord, V> extends
    OraOopOutputFormatBase<K, V> {

  private static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopOutputFormatInsert.class);

  /**
   * Type of insert to use - direct or partition exchange load.
   */
  public enum InsertMode {
    DirectInsert, ExchangePartition
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    OraOopUtilities.checkJavaSecurityEgd();
    Configuration conf = context.getConfiguration();

    int mapperId = this.getMapperId(context);
    applyMapperJdbcUrl(context, mapperId);

    // Is each mapper inserting rows into a unique table?...
    InsertMode insertMode = OraOopUtilities.getExportInsertMode(conf);

    // Should we use the APPEND_VALUES Oracle hint?...
    boolean useAppendValuesOracleHint = false;
    if (insertMode == InsertMode.ExchangePartition) {
      // NB: "Direct inserts" cannot utilize APPEND_VALUES, otherwise Oracle
      // will serialize
      // the N mappers, causing a lot of lock contention.
      useAppendValuesOracleHint = this.canUseOracleAppendValuesHint(context);
    }

    // Has the user forced the use of APPEND_VALUES either on or off?...
    useAppendValuesOracleHint =
        allowUserToOverrideUseOfTheOracleAppendValuesHint(context,
            useAppendValuesOracleHint);

    // If using APPEND_VALUES, check the batch size and commit frequency...
    if (useAppendValuesOracleHint) {
      updateBatchSizeInConfigurationToAllowOracleAppendValuesHint(context);
    }

    // Create the Record Writer...
    OraOopDBRecordWriterInsert result = null;
    try {
      result =
          new OraOopDBRecordWriterInsert(context, mapperId, insertMode,
              useAppendValuesOracleHint);
    } catch (NoClassDefFoundError ex) {
      throw new IOException(String.format(
          "Unable to create an instance of OraOopDBRecordWriterInsert.\n"
              + "The classpath is:\n%s", OraOopUtilities.getJavaClassPath()),
          ex);
    } catch (Exception ex) {
      throw new IOException(ex);
    }

    try {
      result.getExportTableAndColumns(context);
    } catch (SQLException ex) {
      throw new IOException(ex);
    }

    return result;
  }

  /**
   * Insert into an Oracle table based on emitted keys.
   */
  public class OraOopDBRecordWriterInsert extends OraOopDBRecordWriterBase {

    private String sqlStatement; // <- The SQL used when inserting batches of
                                 // rows into the Oracle table
    private InsertMode insertMode; // <- The modus operandi of this class. i.e.
                                   // Whether we insert into the Oracle table
                                   // directly, or insert data into a separate
                                   // table and then perform an EXCHANGE
                                   // PARTITION statement.
    private boolean useAppendValuesOracleHint; // <- Whether to use the
                                               // " /*+APPEND_VALUES*/ " hint
                                               // within the Oracle SQL
                                               // statement we generate
    private String subPartitionName; // <- The name of the subpartition in the
                                     // "main table" that this mappers unique
                                     // table will be exchanged with

    public OraOopDBRecordWriterInsert(TaskAttemptContext context, int mapperId,
        InsertMode insertMode, boolean useAppendValuesOracleHint)
        throws ClassNotFoundException, SQLException {

      super(context, mapperId);
      this.insertMode = insertMode;
      this.useAppendValuesOracleHint = useAppendValuesOracleHint;
    }

    @Override
    protected void getExportTableAndColumns(TaskAttemptContext context)
        throws SQLException {

      Configuration conf = context.getConfiguration();

      switch (this.insertMode) {

        case DirectInsert:
          super.getExportTableAndColumns(context);
          break;

        case ExchangePartition:
          // This mapper inserts data into a unique table before exchanging it
          // into
          // a subpartition of the 'real' export table...

          this.oracleTable = createUniqueMapperTable(context);
          setOracleTableColumns(OraOopOracleQueries.getTableColumns(this
              .getConnection(), this.oracleTable, OraOopUtilities
              .omitLobAndLongColumnsDuringImport(conf), OraOopUtilities
              .recallSqoopJobType(conf), true // <- onlyOraOopSupportedTypes
              , false) // <- omitOraOopPseudoColumns
          );

          this.subPartitionName =
              OraOopUtilities.generateExportTableSubPartitionName(
                  this.mapperId, this.getJobSysDate(context), conf);

          break;

        default:
          throw new RuntimeException(String.format(
              "Update %s to cater for the insertMode \"%s\".", OraOopUtilities
                  .getCurrentMethodName(), this.insertMode.toString()));
      }

    }

    @Override
    public void closeConnection(TaskAttemptContext context)
        throws SQLException {

      // If this mapper is inserting data into a unique table, we'll now
      // move this data into the main export table...
      if (this.insertMode == InsertMode.ExchangePartition) {

        // Perform an "exchange subpartition" operation on the "main table"
        // to convert this table into a subpartition of the "main table"...
        exchangePartitionUniqueMapperTableDataIntoMainExportTable(context);

        LOG.debug(String.format("Dropping temporary mapper table %s",
            this.oracleTable.toString()));
        OraOopOracleQueries.dropTable(this.getConnection(), this.oracleTable);
      }

      super.closeConnection(context);
    }

    private void exchangePartitionUniqueMapperTableDataIntoMainExportTable(
        TaskAttemptContext context) throws SQLException {

      String schema =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_OWNER);
      String localTableName =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_NAME);
      OracleTable mainTable = new OracleTable(schema, localTableName);

      try {
        long start = System.nanoTime();

        OraOopOracleQueries.exchangeSubpartition(this.getConnection(),
            mainTable, this.subPartitionName, this.oracleTable);

        double timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
        LOG.info(String
            .format(
                "Time spent performing an \"exchange subpartition with "
                + "table\": %f sec.",
                timeInSec));
      } catch (SQLException ex) {
        throw new SQLException(
            String
                .format(
                    "Unable to perform an \"exchange subpartition\" operation "
                        + "for the table %s, for the subpartition named "
                        + "\"%s\" with the table named \"%s\".",
                    mainTable.toString(), this.subPartitionName,
                    this.oracleTable.toString()), ex);
      }
    }

    @Override
    protected String getBatchSqlStatement() {

      if (sqlStatement == null) {
        this.sqlStatement =
            getBatchInsertSqlStatement(this.useAppendValuesOracleHint
                ? "/*+APPEND_VALUES*/" : "");
      }

      return this.sqlStatement;
    }

    @Override
    void configurePreparedStatement(PreparedStatement statement,
        List<SqoopRecord> userRecords) throws SQLException {

      Map<String, Object> fieldMap;
      try {
        for (SqoopRecord record : userRecords) {
          fieldMap = record.getFieldMap();

          configurePreparedStatementColumns(statement, fieldMap);
        }

      } catch (Exception ex) {
        if (ex instanceof SQLException) {
          throw (SQLException) ex;
        } else {
          LOG.error(String.format("The following error occurred during %s",
              OraOopUtilities.getCurrentMethodName()), ex);
          throw new SQLException(ex);
        }
      }

    }

  }

}

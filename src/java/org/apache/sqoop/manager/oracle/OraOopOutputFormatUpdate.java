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
import org.apache.sqoop.manager.oracle.OraOopOracleQueries.
           CreateExportChangesTableOptions;

/**
 * Update an Oracle table based on emitted keys.
 */
public class OraOopOutputFormatUpdate<K extends SqoopRecord, V> extends
    OraOopOutputFormatBase<K, V> {

  private static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopOutputFormatUpdate.class);

  /**
   * Type of export - straight update or merge (update-insert).
   */
  public enum UpdateMode {
    Update, Merge
  }

  private enum ExportTableUpdateTechnique {
    ReInsertUpdatedRows, ReInsertUpdatedRowsAndNewRows, UpdateSql, MergeSql
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    OraOopUtilities.checkJavaSecurityEgd();
    Configuration conf = context.getConfiguration();

    // Get the unique JDBC URL to use for this mapper and update the
    // configuration property
    // so that the URL is actually used...
    int mapperId = this.getMapperId(context);
    applyMapperJdbcUrl(context, mapperId);

    UpdateMode updateMode = OraOopUtilities.getExportUpdateMode(conf);

    boolean useAppendValuesOracleHint = false;

    if (updateMode == UpdateMode.Merge || updateMode == UpdateMode.Update) {
      // Should we use the APPEND_VALUES Oracle hint?...
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
    OraOopDBRecordWriterUpdate result = null;
    try {
      result =
          new OraOopDBRecordWriterUpdate(context, mapperId, updateMode,
              useAppendValuesOracleHint);
    } catch (NoClassDefFoundError ex) {
      throw new IOException(String.format(
          "Unable to create an instance of OraOopDBRecordWriterUpdate.\n"
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
   * Update an Oracle table based on emitted keys.
   */
  public class OraOopDBRecordWriterUpdate extends OraOopDBRecordWriterBase {

    private String sqlStatement; // <- The SQL used when updating batches of
                                 // rows into the Oracle table
    private String[] updateColumnNames; // <- The name of the column(s) used to
                                        // match a row in the HDFS file to a row
                                        // in the Oracle table. i.e. What as
                                        // specified in the "--update-key" sqoop
                                        // argument.
    private UpdateMode updateMode; // <- The modus operandi of this class. i.e.
                                   // Whether we update the Oracle table
                                   // directly, or insert data into a separate
                                   // table and then apply a SQL MERGE
                                   // statement.
    private boolean useAppendValuesOracleHint; // <- Whether to use the
                                               // " /*+APPEND_VALUES*/ " hint
                                               // within the Oracle SQL
                                               // statement we generate
    private boolean tableHasOraOopPartitions; // <- Indicates whether the export
                                              // table has partitions that were
                                              // creted by OraOop
    private long numberOfRowsSkipped; // <- The number of rows encountered
                                      // during configurePreparedStatement()
                                      // that had a NULL value for (one of) the
                                      // update columns. This row was therefore
                                      // skipped.

    public OraOopDBRecordWriterUpdate(TaskAttemptContext context, int mapperId,
        UpdateMode updateMode, boolean useAppendValuesOracleHint)
        throws ClassNotFoundException, SQLException {

      super(context, mapperId);

      Configuration conf = context.getConfiguration();

      this.updateColumnNames =
          OraOopUtilities.getExportUpdateKeyColumnNames(conf);
      this.useAppendValuesOracleHint = useAppendValuesOracleHint;
      this.updateMode = updateMode;
      this.tableHasOraOopPartitions =
          conf.getBoolean(OraOopConstants.EXPORT_TABLE_HAS_ORAOOP_PARTITIONS,
              false);
    }

    @Override
    protected void getExportTableAndColumns(TaskAttemptContext context)
        throws SQLException {

      Configuration conf = context.getConfiguration();

      this.oracleTable = createUniqueMapperTable(context);
      setOracleTableColumns(OraOopOracleQueries.getTableColumns(this
          .getConnection(), this.oracleTable, OraOopUtilities
          .omitLobAndLongColumnsDuringImport(conf), OraOopUtilities
          .recallSqoopJobType(conf), true // <- onlyOraOopSupportedTypes
          , false) // <- omitOraOopPseudoColumns
      );
    }

    @Override
    public void closeConnection(TaskAttemptContext context)
        throws SQLException {

      try {

        if (this.numberOfRowsSkipped > 0) {
          LOG.warn(String.format(
              "%d records were skipped due to a NULL value within one of the "
            + "update-key column(s).\nHaving a NULL value prevents a record "
            + "from being able to be matched to a row in the Oracle table.",
                  this.numberOfRowsSkipped));
        }

        // Now update the "main" export table with data that was inserted into
        // this mapper's table...
        updateMainExportTableFromUniqueMapperTable(context,
            this.updateColumnNames);

        LOG.debug(String.format("Dropping temporary mapper table %s",
            this.oracleTable.toString()));
        OraOopOracleQueries.dropTable(this.getConnection(), this.oracleTable);
      } finally {
        super.closeConnection(context);
      }
    }

    private ExportTableUpdateTechnique getExportTableUpdateTechnique() {

      ExportTableUpdateTechnique result;

      if (this.tableHasOraOopPartitions) {
        switch (this.updateMode) {

          case Update:
            result = ExportTableUpdateTechnique.ReInsertUpdatedRows;
            break;

          case Merge:
            result = ExportTableUpdateTechnique.ReInsertUpdatedRowsAndNewRows;
            break;

          default:
            throw new RuntimeException(String.format(
                "Update %s to cater for the updateMode \"%s\".",
                OraOopUtilities.getCurrentMethodName(), this.updateMode
                    .toString()));
        }
      } else {
        switch (this.updateMode) {

          case Update:
            result = ExportTableUpdateTechnique.UpdateSql;
            break;

          case Merge:
            result = ExportTableUpdateTechnique.MergeSql;
            break;

          default:
            throw new RuntimeException(String.format(
                "Update %s to cater for the updateMode \"%s\".",
                OraOopUtilities.getCurrentMethodName(), this.updateMode
                    .toString()));
        }
      }

      return result;
    }

    private void updateMainExportTableFromUniqueMapperTable(
        TaskAttemptContext context, String[] mergeColumnNames)
        throws SQLException {

      String schema =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_OWNER);
      String localTableName =
          context.getConfiguration().get(OraOopConstants.ORAOOP_TABLE_NAME);
      OracleTable targetTable = new OracleTable(schema, localTableName);

      Object sysDateTime = getJobSysDate(context);
      OracleTable changesTable =
          OraOopUtilities.generateExportTableMapperTableName(Integer
              .toString(this.mapperId)
              + "_CHG", sysDateTime, null);

      OraOopOracleQueries.CreateExportChangesTableOptions changesTableOptions;
      boolean parallelizationEnabled =
          OraOopUtilities.enableOracleParallelProcessingDuringExport(context
              .getConfiguration());

      ExportTableUpdateTechnique exportTableUpdateTechnique =
          getExportTableUpdateTechnique();
      switch (exportTableUpdateTechnique) {

        case ReInsertUpdatedRows:
        case UpdateSql:
          changesTableOptions =
              CreateExportChangesTableOptions.OnlyRowsThatDiffer;
          break;

        case ReInsertUpdatedRowsAndNewRows:
        case MergeSql:
          changesTableOptions =
              CreateExportChangesTableOptions.RowsThatDifferPlusNewRows;
          break;

        default:
          throw new RuntimeException(String.format(
              "Update %s to cater for the ExportTableUpdateTechnique \"%s\".",
              OraOopUtilities.getCurrentMethodName(),
              exportTableUpdateTechnique.toString()));
      }

      String temporaryTableStorageClause =
          OraOopUtilities.getTemporaryTableStorageClause(context
              .getConfiguration());

      try {
        int changeTableRowCount =
            OraOopOracleQueries.createExportChangesTable(this.getConnection(),
                changesTable, temporaryTableStorageClause, this.oracleTable,
                targetTable, this.updateColumnNames, changesTableOptions,
                parallelizationEnabled);

        if (changeTableRowCount == 0) {
          LOG.debug(String.format(
              "The changes-table does not contain any rows. %s is now exiting.",
                  OraOopUtilities.getCurrentMethodName()));
          return;
        }

        switch (exportTableUpdateTechnique) {

          case ReInsertUpdatedRows:
          case ReInsertUpdatedRowsAndNewRows:

            OraOopOracleQueries.deleteRowsFromTable(this.getConnection(),
                targetTable, changesTable, this.updateColumnNames,
                parallelizationEnabled);

            OraOopOracleQueries.insertRowsIntoExportTable(this.getConnection(),
                targetTable, changesTable, sysDateTime, this.mapperId,
                parallelizationEnabled);
            break;

          case UpdateSql:

            long start = System.nanoTime();

            OraOopOracleQueries.updateTable(this.getConnection(), targetTable,
                changesTable, this.updateColumnNames, this
                    .getOracleTableColumns(), sysDateTime, this.mapperId,
                parallelizationEnabled);

            double timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
            LOG.info(String.format("Time spent performing an update: %f sec.",
                timeInSec));
            break;

          case MergeSql:

            long mergeStart = System.nanoTime();

            OraOopOracleQueries.mergeTable(this.getConnection(), targetTable,
                changesTable, this.updateColumnNames, this
                    .getOracleTableColumns(), sysDateTime, this.mapperId,
                parallelizationEnabled);

            double mergeTimeInSec = (System.nanoTime() - mergeStart)
                / Math.pow(10, 9);
            LOG.info(String.format("Time spent performing a merge: %f sec.",
                mergeTimeInSec));

            break;

          default:
            throw new RuntimeException(
              String.format(
                "Update %s to cater for the ExportTableUpdateTechnique \"%s\".",
                        OraOopUtilities.getCurrentMethodName(),
                        exportTableUpdateTechnique.toString()));
        }

        this.getConnection().commit();
      } catch (SQLException ex) {
        this.getConnection().rollback();
        throw ex;
      } finally {
        OraOopOracleQueries.dropTable(this.getConnection(), changesTable);
      }
    }

    @Override
    protected String getBatchSqlStatement() {

      if (sqlStatement == null) {
        this.sqlStatement =
            getBatchInsertSqlStatement(
                this.useAppendValuesOracleHint ? "/*+APPEND_VALUES*/"
                : "");
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

          boolean updateKeyValueIsNull = false;
          for (int idx = 0; idx < this.updateColumnNames.length; idx++) {
            String updateColumnName = this.updateColumnNames[idx];
            Object updateKeyValue = fieldMap.get(updateColumnName);
            if (updateKeyValue == null) {
              this.numberOfRowsSkipped++;
              updateKeyValueIsNull = true;
              break;
            }
          }

          if (updateKeyValueIsNull) {
            continue;
          }

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

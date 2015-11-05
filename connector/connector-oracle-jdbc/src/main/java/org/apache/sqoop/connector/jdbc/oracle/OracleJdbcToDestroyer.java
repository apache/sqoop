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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries.CreateExportChangesTableOptions;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTableColumns;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.ExportTableUpdateTechnique;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.InsertMode;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.UpdateMode;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;

public class OracleJdbcToDestroyer extends Destroyer<LinkConfiguration, ToJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(OracleJdbcToDestroyer.class);

  protected Connection connection;
  protected OracleTable table;
  protected int numMappers = 8;

  private void connect(ConnectionConfig connectionConfig) {
    try {
      connection = OracleConnectionFactory.makeConnection(connectionConfig);
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\n"
              + "Error:%s", connectionConfig.connectionString, ex
              .getMessage()), ex);
    }
  }

  @Override
  public void destroy(DestroyerContext context,
      LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) {
    LOG.debug("Running Oracle JDBC connector destroyer");

    table = OracleUtilities.decodeOracleTableName(
        linkConfiguration.connectionConfig.username,
        jobConfiguration.toJobConfig.tableName);

    if (jobConfiguration.toJobConfig.updateKey == null ||
        jobConfiguration.toJobConfig.updateKey.isEmpty()) {

      // Is each mapper inserting rows into a unique table?...
      InsertMode insertMode = OracleUtilities.getExportInsertMode(
          jobConfiguration.toJobConfig, context.getContext());

      if(insertMode==InsertMode.ExchangePartition) {
        connect(linkConfiguration.connectionConfig);
        Object sysDateTime =
            OracleUtilities.recallOracleDateTime(context.getContext());

        exchangePartitionUniqueMapperTableDataIntoMainExportTable(sysDateTime);

      }

    } else {
      connect(linkConfiguration.connectionConfig);
      Object sysDateTime =
          OracleUtilities.recallOracleDateTime(context.getContext());
      try {
        updateMainExportTableFromUniqueMapperTable(jobConfiguration.toJobConfig,
            context.getContext(), sysDateTime);
      } catch(SQLException e) {
        throw new RuntimeException(
            String.format(
                "Unable to update the table %s.",table.toString()), e);
      }
    }
  }

  private void exchangePartitionUniqueMapperTableDataIntoMainExportTable(
      Object sysDateTime) {

    for(int i=0; i<numMappers; i++) {
        long start = System.nanoTime();

        OracleTable mapperTable =
            OracleUtilities.generateExportTableMapperTableName(
                i, sysDateTime, null);

        String subPartitionName =
            OracleUtilities.generateExportTableSubPartitionName(
                i, sysDateTime);

        try {
          OracleQueries.exchangeSubpartition(connection,
              table, subPartitionName, mapperTable);

          double timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
          LOG.info(String
              .format(
                  "Time spent performing an \"exchange subpartition with "
                  + "table\": %f sec.",
                  timeInSec));

          LOG.debug(String.format("Dropping temporary mapper table %s",
              mapperTable.toString()));
          OracleQueries.dropTable(connection, mapperTable);
      } catch (SQLException ex) {
        throw new RuntimeException(
            String
                .format(
                    "Unable to perform an \"exchange subpartition\" operation "
                        + "for the table %s, for the subpartition named "
                        + "\"%s\" with the table named \"%s\".",
                    table.toString(), subPartitionName,
                    mapperTable.toString()), ex);
      }
    }
  }

  private void updateMainExportTableFromUniqueMapperTable(ToJobConfig jobConfig,
      ImmutableContext context, Object sysDateTime)
      throws SQLException {

    String[] updateColumnNames = OracleUtilities.
        getExportUpdateKeyColumnNames(jobConfig);

    OracleTableColumns tableColumns = OracleQueries.getToTableColumns(
        connection, table, true, false);

    UpdateMode updateMode = OracleUtilities.getExportUpdateMode(jobConfig);

    ExportTableUpdateTechnique exportTableUpdateTechnique =
        OracleUtilities.getExportTableUpdateTechnique(context, updateMode);

    CreateExportChangesTableOptions changesTableOptions;
    boolean parallelizationEnabled =
        OracleUtilities.enableOracleParallelProcessingDuringExport(jobConfig);

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
            OracleUtilities.getCurrentMethodName(),
            exportTableUpdateTechnique.toString()));
    }

    String temporaryTableStorageClause =
        OracleUtilities.getTemporaryTableStorageClause(jobConfig);

    for(int i=0; i<numMappers; i++) {

      OracleTable mapperTable =
          OracleUtilities.generateExportTableMapperTableName(
              i, sysDateTime, null);

      OracleTable changesTable =
          OracleUtilities.generateExportTableMapperTableName(Integer
              .toString(i) + "_CHG", sysDateTime, null);

      try {
        int changeTableRowCount =
            OracleQueries.createExportChangesTable(connection,
                changesTable, temporaryTableStorageClause, mapperTable,
                table, updateColumnNames, changesTableOptions,
                parallelizationEnabled);

        if (changeTableRowCount == 0) {
          LOG.debug(String.format(
              "The changes-table does not contain any rows. %s is now exiting.",
                  OracleUtilities.getCurrentMethodName()));
          continue;
        }

        switch (exportTableUpdateTechnique) {

          case ReInsertUpdatedRows:
          case ReInsertUpdatedRowsAndNewRows:

            OracleQueries.deleteRowsFromTable(connection,
                table, changesTable, updateColumnNames,
                parallelizationEnabled);

            OracleQueries.insertRowsIntoExportTable(connection,
                table, changesTable, sysDateTime, i,
                parallelizationEnabled);
            break;

          case UpdateSql:

            long start = System.nanoTime();

            OracleQueries.updateTable(connection, table,
                changesTable, updateColumnNames, tableColumns, sysDateTime, i,
                parallelizationEnabled);

            double timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
            LOG.info(String.format("Time spent performing an update: %f sec.",
                timeInSec));
            break;

          case MergeSql:

            long mergeStart = System.nanoTime();

            OracleQueries.mergeTable(connection, table,
                changesTable, updateColumnNames, tableColumns, sysDateTime,
                i, parallelizationEnabled);

            double mergeTimeInSec = (System.nanoTime() - mergeStart)
                / Math.pow(10, 9);
            LOG.info(String.format("Time spent performing a merge: %f sec.",
                mergeTimeInSec));

            break;

          default:
            throw new RuntimeException(
              String.format(
                "Update %s to cater for the ExportTableUpdateTechnique \"%s\".",
                        OracleUtilities.getCurrentMethodName(),
                        exportTableUpdateTechnique.toString()));
        }

        connection.commit();
      } catch (SQLException ex) {
        connection.rollback();
        throw ex;
      } finally {
        OracleQueries.dropTable(connection, changesTable);
        LOG.debug(String.format("Dropping temporary mapper table %s",
            mapperTable.toString()));
        OracleQueries.dropTable(connection, mapperTable);
      }
    }
  }

}

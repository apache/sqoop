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
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTablePartition;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTablePartitions;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.UpdateMode;
import org.apache.sqoop.job.etl.InitializerContext;

public class OracleJdbcToInitializer extends
    OracleJdbcCommonInitializer<ToJobConfiguration> {

  private static final Logger LOG =
    Logger.getLogger(OracleJdbcToInitializer.class);

  @Override
  public void connect(InitializerContext context,
      LinkConfiguration linkConfiguration, ToJobConfiguration jobConfiguration)
          throws SQLException {
    super.connect(context, linkConfiguration, jobConfiguration);
    table = OracleUtilities.decodeOracleTableName(
        linkConfiguration.connectionConfig.username,
        jobConfiguration.toJobConfig.tableName);
  }

  @Override
  public void initialize(InitializerContext context,
      LinkConfiguration linkConfiguration,
      ToJobConfiguration jobConfiguration) {
    super.initialize(context, linkConfiguration, jobConfiguration);
    LOG.debug("Running Oracle JDBC connector initializer");
    try {
      createAnyRequiredOracleObjects(context.getContext(),
          jobConfiguration.toJobConfig, linkConfiguration.connectionConfig);

      if (!isSqoopTableAnOracleTable(connection,
          linkConfiguration.connectionConfig.username, table)) {
        throw new RuntimeException("Can only load data into Oracle tables.");
      }
    } catch(SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createAnyRequiredOracleObjects(MutableContext context,
      ToJobConfig jobConfig, ConnectionConfig connectionConfig)
          throws SQLException {

      // The SYSDATE on the Oracle database will be used as the partition value
      // for this export job...
      Object sysDateTime = OracleQueries.getSysDate(connection);
      String sysDateStr =
        OracleQueries.oraDATEToString(sysDateTime, "yyyy-mm-dd hh24:mi:ss");
      context.setString(OracleJdbcConnectorConstants.SQOOP_ORACLE_JOB_SYSDATE,
          sysDateStr);

      checkForOldOraOopTemporaryOracleTables(connection, sysDateTime,
          OracleQueries.getCurrentSchema(connection));

      // Store the actual partition value, so the N mappers know what value to
      // insert...
      String partitionValue =
          OracleQueries.oraDATEToString(sysDateTime,
              OracleJdbcConnectorConstants.ORAOOP_EXPORT_PARTITION_DATE_FORMAT);
      context.setString(
          OracleJdbcConnectorConstants.ORAOOP_EXPORT_PARTITION_DATE_VALUE,
          partitionValue);

      // Generate the (22 character) partition name...
      String partitionName =
          OracleUtilities
              .createExportTablePartitionNameFromOracleTimestamp(sysDateTime);

      //TODO: Number of mappers needs to be fixed
      int numMappers = 8;

      String exportTableTemplate = jobConfig.templateTable;

      if(exportTableTemplate==null) {
        exportTableTemplate = "";
      }

      String user = connectionConfig.username;
      //TODO: This is from the other Oracle Manager
      //if (user == null) {
      //  user = OracleManager.getSessionUser(connection);
      //}

      OracleTable templateTableContext =
          OracleUtilities.decodeOracleTableName(user, exportTableTemplate);

      boolean noLoggingOnNewTable = BooleanUtils.isTrue(jobConfig.nologging);

      List<String> updateKeyCol = jobConfig.updateKey;

      /* =========================== */
      /* VALIDATION OF INPUTS */
      /* =========================== */

      if (updateKeyCol == null || updateKeyCol.isEmpty()) {
        // We're performing an "insert" export, not an "update" export.

        // Check that the "oraoop.export.merge" property has not been specified,
        // as this would be
        // an invalid scenario...
        if (OracleUtilities.getExportUpdateMode(jobConfig) == UpdateMode.Merge) {
          throw new RuntimeException("The merge option can only be used if "
              + "an update key is specified.");
        }
      }

      if (OracleUtilities
          .userWantsToCreatePartitionedExportTableFromTemplate(jobConfig)
          || OracleUtilities
              .userWantsToCreateNonPartitionedExportTableFromTemplate(jobConfig)) {

        // OraOop will create the export table.

        if (table.getName().length()
                > OracleJdbcConnectorConstants.Oracle.MAX_IDENTIFIER_LENGTH) {
          String msg =
              String.format(
                  "The Oracle table name \"%s\" is longer than %d characters.\n"
                + "Oracle will not allow a table with this name to be created.",
              table.getName(),
              OracleJdbcConnectorConstants.Oracle.MAX_IDENTIFIER_LENGTH);
          throw new RuntimeException(msg);
        }

        if (updateKeyCol != null && !updateKeyCol.isEmpty()) {

          // We're performing an "update" export, not an "insert" export.

          // Check whether the user is attempting an "update" (i.e. a non-merge).
          // If so, they're
          // asking to only UPDATE rows in a (about to be created) (empty) table
          // that contains no rows.
          // This will be a waste of time, as we'd be attempting to perform UPDATE
          // operations against a
          // table with no rows in it...
          UpdateMode updateMode = OracleUtilities.getExportUpdateMode(jobConfig);
          if (updateMode == UpdateMode.Update) {
            throw new RuntimeException(String.format(
                "\n\nCombining the template table option with the merge "
              + "option is nonsensical, as this would create an "
              + "empty table and then perform "
              + "a lot of work that results in a table containing no rows.\n"));
          }
        }

        // Check that the specified template table actually exists and is a
        // table...
        String templateTableObjectType =
            OracleQueries.getOracleObjectType(connection,
                templateTableContext);
        if (templateTableObjectType == null) {
          throw new RuntimeException(String.format(
              "The specified Oracle template table \"%s\" does not exist.",
              templateTableContext.toString()));
        }

        if (!templateTableObjectType.equalsIgnoreCase(
                OracleJdbcConnectorConstants.Oracle.OBJECT_TYPE_TABLE)) {
          throw new RuntimeException(
            String.format(
                "The specified Oracle template table \"%s\" is not an "
              + "Oracle table, it's a %s.",
                templateTableContext.toString(), templateTableObjectType));
        }

        if (BooleanUtils.isTrue(jobConfig.dropTableIfExists)) {
          OracleQueries.dropTable(connection, table);
        }

        // Check that there is no existing database object with the same name of
        // the table to be created...
        String newTableObjectType =
            OracleQueries.getOracleObjectType(connection, table);
        if (newTableObjectType != null) {
          throw new RuntimeException(
            String.format(
                "%s cannot create a new Oracle table named %s as a \"%s\" "
              + "with this name already exists.",
              OracleJdbcConnectorConstants.CONNECTOR_NAME, table.toString(),
              newTableObjectType));
        }
      } else {
        // The export table already exists.

        if (updateKeyCol != null && !updateKeyCol.isEmpty()) {

          // We're performing an "update" export, not an "insert" export.

          // Check that there exists an index on the export table on the
          // update-key column(s).
          // Without such an index, this export may perform like a real dog...
          String[] updateKeyColumns =
              OracleUtilities.getExportUpdateKeyColumnNames(jobConfig);
          if (!OracleQueries.doesIndexOnColumnsExist(connection,
              table, updateKeyColumns)) {
            String msg = String.format(
                "\n**************************************************************"
              + "***************************************************************"
              + "\n\tThe table %1$s does not have a valid index on "
              + "the column(s) %2$s.\n"
              + "\tAs a consequence, this export may take a long time to "
              + "complete.\n"
              + "\tIf performance is unacceptable, consider reattempting this "
              + "job after creating an index "
              + "on this table via the SQL...\n"
              + "\t\tcreate index <index_name> on %1$s(%2$s);\n"
              + "****************************************************************"
              + "*************************************************************",
                        table.toString(),
                        OracleUtilities.stringArrayToCSV(updateKeyColumns));
            LOG.warn(msg);
          }
        }
      }

      boolean createMapperTables = false;

      if (updateKeyCol != null && !updateKeyCol.isEmpty()) {
        createMapperTables = true;
      }

      if (OracleUtilities
          .userWantsToCreatePartitionedExportTableFromTemplate(jobConfig)) {
        /* ================================= */
        /* CREATE A PARTITIONED TABLE */
        /* ================================= */

        // Create a new Oracle table using the specified template...

        String[] subPartitionNames =
            OracleUtilities.generateExportTableSubPartitionNames(numMappers,
                sysDateTime);
        // Create the export table from a template table...
        String tableStorageClause =
            OracleUtilities.getExportTableStorageClause(jobConfig);

        OracleQueries.createExportTableFromTemplateWithPartitioning(
            connection, table,
            tableStorageClause, templateTableContext, noLoggingOnNewTable,
            partitionName, sysDateTime, numMappers,
            subPartitionNames);

        createMapperTables = true;
      } else if (OracleUtilities
          .userWantsToCreateNonPartitionedExportTableFromTemplate(jobConfig)) {
        /* ===================================== */
        /* CREATE A NON-PARTITIONED TABLE */
        /* ===================================== */
        String tableStorageClause =
            OracleUtilities.getExportTableStorageClause(jobConfig);

        OracleQueries.createExportTableFromTemplate(connection,
            table, tableStorageClause,
            templateTableContext, noLoggingOnNewTable);
      } else {
        /* ===================================================== */
        /* ADD ADDITIONAL PARTITIONS TO AN EXISTING TABLE */
        /* ===================================================== */

        // If the export table is partitioned, and the partitions were created by
        // OraOop, then we need
        // create additional partitions...

        OracleTablePartitions tablePartitions =
            OracleQueries.getPartitions(connection, table);
        // Find any partition name starting with "ORAOOP_"...
        OracleTablePartition oraOopPartition =
            tablePartitions.findPartitionByRegEx("^"
                + OracleJdbcConnectorConstants.
                    EXPORT_TABLE_PARTITION_NAME_PREFIX);

        if (tablePartitions.size() > 0 && oraOopPartition == null) {

          for (int idx = 0; idx < tablePartitions.size(); idx++) {
            LOG.info(String.format(
                    "The Oracle table %s has a partition named \"%s\".",
                    table.toString(),
                    tablePartitions.get(idx).getName()));
          }

          LOG.warn(String.format(
                  "The Oracle table %s is partitioned.\n"
                      + "These partitions were not created by %s.",
                  table.toString(),
                  OracleJdbcConnectorConstants.CONNECTOR_NAME));
        }

        if (oraOopPartition != null) {

          // Indicate in the configuration what's happening...
          context.setBoolean(OracleJdbcConnectorConstants.
              EXPORT_TABLE_HAS_SQOOP_PARTITIONS, true);

          LOG.info(String.format(
                          "The Oracle table %s is partitioned.\n"
                              + "These partitions were created by %s, so "
                              + "additional partitions will now be created.\n"
                              + "The name of the new partition will be \"%s\".",
                          table.toString(), OracleJdbcConnectorConstants.
                          CONNECTOR_NAME, partitionName));

          String[] subPartitionNames =
              OracleUtilities.generateExportTableSubPartitionNames(numMappers,
                  sysDateTime);

          // Add another partition (and N subpartitions) to this existing,
          // partitioned export table...
          OracleQueries.createMoreExportTablePartitions(connection,
              table, partitionName,
              sysDateTime, subPartitionNames);

          createMapperTables = true;
        }
      }

      if(createMapperTables) {
        createUniqueMapperTable(sysDateTime, numMappers, jobConfig);
      }
    }

  private void createUniqueMapperTable(Object sysDateTime,
      int numMappers, ToJobConfig jobConfig)
      throws SQLException {

    // Mappers insert data into a unique table before either:
    // - exchanging it into a subpartition of the 'real' export table; or
    // - merging it into the 'real' export table.

    for (int i=0; i<numMappers; i++) {
      OracleTable mapperTable =
          OracleUtilities.generateExportTableMapperTableName(i,
              sysDateTime, null);

      // If this mapper is being reattempted in response to a failure, we need
      // to delete the
      // temporary table created by the previous attempt...
      OracleQueries.dropTable(connection, mapperTable);

      String temporaryTableStorageClause =
          OracleUtilities.getTemporaryTableStorageClause(jobConfig);

      OracleQueries.createExportTableForMapper(connection,
          mapperTable, temporaryTableStorageClause, table
          , false); // <- addOraOopPartitionColumns

      LOG.debug(String.format("Created temporary mapper table %s", mapperTable
          .toString()));
    }
  }

  private void checkForOldOraOopTemporaryOracleTables(Connection connection,
      Object sysDateTime, String schema) {

    try {

      StringBuilder message = new StringBuilder();
      message
        .append(String.format(
          "The following tables appear to be old temporary tables created by "
        + "%s that have not been deleted.\n"
        + "They are probably left over from jobs that encountered an error and "
        + "could not clean up after themselves.\n"
        + "You might want to drop these Oracle tables in order to reclaim "
        + "Oracle storage space:\n",
        OracleJdbcConnectorConstants.CONNECTOR_NAME));
      boolean showMessage = false;

      String generatedTableName =
          OracleUtilities.generateExportTableMapperTableName(0, sysDateTime,
              schema).getName();
      generatedTableName = generatedTableName.replaceAll("[0-9]", "%");
      generatedTableName =
          OracleUtilities.replaceAll(generatedTableName, "%%", "%");
      Date sysDate = OracleQueries.oraDATEToDate(sysDateTime);

      List<OracleTable> tables =
          OracleQueries.getTablesWithTableNameLike(connection, schema,
              generatedTableName);

      for (OracleTable oracleTable : tables) {
        OracleUtilities.DecodedExportMapperTableName tableName =
            OracleUtilities.decodeExportTableMapperTableName(oracleTable);
        if (tableName != null) {
          Date tableDate =
              OracleQueries.oraDATEToDate(tableName.getTableDateTime());
          double daysApart =
              (sysDate.getTime() - tableDate.getTime()) / (1000 * 60 * 60 * 24);
          if (daysApart > 1.0) {
            showMessage = true;
            message.append(String.format("\t%s\n", oracleTable.toString()));
          }
        }
      }

      if (showMessage) {
        LOG.info(message.toString());
      }
    } catch (Exception ex) {
      LOG.warn(String.format(
              "%s was unable to check for the existance of old "
                  + "temporary Oracle tables.\n" + "Error:\n%s",
              OracleJdbcConnectorConstants.CONNECTOR_NAME, ex.toString()));
    }
  }

  private boolean isSqoopTableAnOracleTable(Connection connection,
      String connectionUserName, OracleTable tableContext) {

    String oracleObjectType;

    try {

      // Find the table via dba_tables...
      OracleTable oracleTable =
          OracleQueries.getTable(connection, tableContext.getSchema(),
              tableContext.getName());
      if (oracleTable != null) {
        return true;
      }

      // If we could not find the table via dba_tables, then try and determine
      // what type of database object the
      // user was referring to. Perhaps they've specified the name of a view?...
      oracleObjectType =
          OracleQueries.getOracleObjectType(connection, tableContext);

      if (oracleObjectType == null) {
        LOG.info(String.format(
            "%1$s will not process this Sqoop connection, "
          + "as the Oracle user %2$s does not own a table named %3$s.\n"
          + "\tPlease prefix the table name with the owner.\n "
          + "\tNote: You may need to double-quote the owner and/or table name."
          + "\n\tE.g. sqoop ... --username %4$s --table %2$s.%3$s\n",
          OracleJdbcConnectorConstants.CONNECTOR_NAME, tableContext.getSchema(),
          tableContext.getName(), connectionUserName));
        return false;
      }

    } catch (SQLException ex) {
      LOG.warn(String.format(
        "Unable to determine the Oracle-type of the object named %s owned by "
            + "%s.\nError:\n" + "%s", tableContext.getName(), tableContext
            .getSchema(), ex.getMessage()));

      // In the absence of conflicting information, let's assume the object is
      // actually a table...
      return true;
    }

    boolean result =
        oracleObjectType.equalsIgnoreCase(
            OracleJdbcConnectorConstants.Oracle.OBJECT_TYPE_TABLE);

    if (!result) {
      LOG.info(String.format("%s will not process this sqoop connection, "
          + "as %s is not an Oracle table, it's a %s.",
          OracleJdbcConnectorConstants.CONNECTOR_NAME, tableContext.toString(),
          oracleObjectType));
    }

    return result;
  }

  @Override
  protected List<String> getColumnNames(ToJobConfiguration jobConfiguration)
      throws SQLException {
    List<String> colNames = OracleQueries.getToTableColumnNames(
        connection, table, true, true);

    return OracleUtilities.getSelectedColumnNamesInOracleTable(table,
        colNames, jobConfiguration.toJobConfig.columns);
  }
}

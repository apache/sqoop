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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.lang.BooleanUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleDataChunk;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTableColumn;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTableColumns;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.ColumnType;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class OracleJdbcExtractor extends
    Extractor<LinkConfiguration, FromJobConfiguration, OracleJdbcPartition> {

  private static final Logger LOG = Logger.getLogger(OracleJdbcExtractor.class);

  private Connection connection;
  private OracleTable table;
  private int mapperId; // <- The index of this Hadoop mapper
  private long rowsRead = 0;

  private OracleTableColumns tableColumns;

  private OracleJdbcPartition dbInputSplit; // <- The split this record-reader
                                            // is working on.
  private int numberOfBlocksInThisSplit; // <- The number of Oracle blocks in
                                         // this Oracle data-chunk.
  private int numberOfBlocksProcessedInThisSplit; // <- How many Oracle blocks
                                                  // we've processed with this
                                                  // record-reader.
  private String currentDataChunkId; // <- The id of the current data-chunk
                                     // being processed
  private ResultSet results; // <- The ResultSet containing the data from the
                             // query returned by getSelectQuery()
  private int columnIndexDataChunkIdZeroBased = -1; // <- The zero-based column
                                                    // index of the
                                                    // data_chunk_id column.
  private boolean progressCalculationErrorLogged; // <- Whether we've logged a
                                                  // problem with the progress
                                                  // calculation during
                                                  // nextKeyValue().
  private Object oraOopOraStats; // <- A reference to the Oracle statistics
                                 // object that is being tracked for this Oracle
                                 // session.
  private boolean profilingEnabled; // <- Whether to collect profiling metrics
  private long timeSpentInNextKeyValueInNanoSeconds; // <- Total time spent in
                                                     // super.nextKeyValue()

  private static final DateTimeFormatter TIMESTAMP_TIMEZONE =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS z");

  @Override
  public void extract(ExtractorContext context,
      LinkConfiguration linkConfiguration,
      FromJobConfiguration jobConfiguration, OracleJdbcPartition partition) {
    //TODO: Mapper ID
    mapperId = 1;
    dbInputSplit = partition;

    // Retrieve the JDBC URL that should be used by this mapper.
    String mapperJdbcUrlPropertyName =
        OracleUtilities.getMapperJdbcUrlPropertyName(mapperId);
    String mapperJdbcUrl = context.getString(mapperJdbcUrlPropertyName, null);

    LOG.debug(String.format("Mapper %d has a JDBC URL of: %s", mapperId,
        mapperJdbcUrl == null ? "<null>" : mapperJdbcUrl));

    try {
      connection = OracleConnectionFactory.createOracleJdbcConnection(
          OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
          mapperJdbcUrl,
          linkConfiguration.connectionConfig.username,
          linkConfiguration.connectionConfig.password);
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\nError:%s",
          linkConfiguration.connectionConfig.connectionString, ex
              .getMessage()), ex);
    }

    table = OracleUtilities.decodeOracleTableName(
        linkConfiguration.connectionConfig.username,
        jobConfiguration.fromJobConfig.tableName);

    try {
    String thisOracleInstanceName =
        OracleQueries.getCurrentOracleInstanceName(connection);

    LOG.info(String.format(
        "This record reader is connected to Oracle via the JDBC URL: \n"
            + "\t\"%s\"\n" + "\tto the Oracle instance: \"%s\"", mapperJdbcUrl,
            thisOracleInstanceName));

    OracleConnectionFactory.initializeOracleConnection(
        connection, linkConfiguration.connectionConfig);
    } catch(SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to initialize connection to the Oracle database at %s\n"
              + "Error:%s",
              linkConfiguration.connectionConfig.connectionString, ex
              .getMessage()), ex);
    }

    try {
      tableColumns =
          OracleQueries.getFromTableColumns(connection, table, OracleUtilities.
              omitLobAndLongColumnsDuringImport(jobConfiguration.fromJobConfig),
              true // <- onlyOraOopSupportedTypes
              );
    } catch (SQLException ex) {
      LOG.error(String.format(
          "Unable to obtain the data-types of the columns in table %s.\n"
              + "Error:\n%s", table.toString(), ex.getMessage()));
      throw new RuntimeException(ex);
    }

    this.numberOfBlocksInThisSplit =
        this.dbInputSplit.getTotalNumberOfBlocksInThisSplit();
    this.numberOfBlocksProcessedInThisSplit = 0;

    extractData(context, jobConfiguration.fromJobConfig);

    try {
      connection.close();
    } catch(SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to close connection to the Oracle database at %s\nError:%s",
          linkConfiguration.connectionConfig.connectionString, ex
              .getMessage()), ex);
    }
  }

  private Object getObjectAtName(ResultSet resultSet,
      OracleTableColumn column, Column sqoopColumn) throws SQLException {
    Object result = null;
    if(sqoopColumn.getType() == ColumnType.TEXT) {
      result = resultSet.getString(column.getName());
    } else if (column.getOracleType() == OracleQueries
        .getOracleType("TIMESTAMP")) {
      Timestamp timestamp = resultSet.getTimestamp(column.getName());
      if(timestamp!=null) {
        result  = LocalDateTime.fromDateFields(timestamp);
      }
    } else if (column.getOracleType() == OracleQueries
            .getOracleType("TIMESTAMPTZ")
        || column.getOracleType() == OracleQueries
            .getOracleType("TIMESTAMPLTZ")) {
      Timestamp timestamp = resultSet.getTimestamp(column.getName());
      if(timestamp!=null) {
        //TODO: BC dates
        String dateTimeStr = resultSet.getString(column.getName());
        result  = DateTime.parse(dateTimeStr, TIMESTAMP_TIMEZONE);
      }
    } else {
      result = resultSet.getObject(column.getName());
    }
    return result;
  }

  private void extractData(ExtractorContext context, FromJobConfig jobConfig) {
    String sql = getSelectQuery(jobConfig, context.getContext());
    Column[] columns = context.getSchema().getColumnsArray();
    int columnCount = columns.length;
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      ResultSet resultSet = statement.executeQuery();

      while(resultSet.next()) {
        Object[] array = new Object[columnCount];
        for(int i = 0; i < columnCount; i++) {
          OracleTableColumn tableColumn =
              tableColumns.findColumnByName(columns[i].getName());
          array[i] = getObjectAtName(resultSet, tableColumn, columns[i]);
        }
        context.getDataWriter().writeArrayRecord(array);
        rowsRead++;
      }

      resultSet.close();
      statement.close();
    } catch (SQLException ex) {
      LOG.error(String.format("Error in %s while executing the SQL query:\n"
          + "%s\n\n" + "%s", OracleUtilities.getCurrentMethodName(), sql, ex
          .getMessage()));
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long getRowsRead() {
    return rowsRead;
  }

  private String getSelectQuery(FromJobConfig jobConfig,
      ImmutableContext context) {

    boolean consistentRead = BooleanUtils.isTrue(jobConfig.consistentRead);
    long consistentReadScn = context.getLong(
        OracleJdbcConnectorConstants.ORACLE_IMPORT_CONSISTENT_READ_SCN, 0L);
    if (consistentRead && consistentReadScn == 0L) {
      throw new RuntimeException("Could not get SCN for consistent read.");
    }

    StringBuilder query = new StringBuilder();

    if (this.dbInputSplit.getDataChunks() == null) {
      String errMsg =
          String.format("The %s does not contain any data-chunks, within %s.",
              this.dbInputSplit.getClass().getName(), OracleUtilities
                  .getCurrentMethodName());
      throw new RuntimeException(errMsg);
    }

    OracleUtilities.OracleTableImportWhereClauseLocation whereClauseLocation =
        OracleUtilities.getTableImportWhereClauseLocation(jobConfig);

    int numberOfDataChunks = this.dbInputSplit.getNumberOfDataChunks();
    for (int idx = 0; idx < numberOfDataChunks; idx++) {

      OracleDataChunk dataChunk =
          this.dbInputSplit.getDataChunks().get(idx);

      if (idx > 0) {
        query.append("UNION ALL \n");
      }

      query.append(getColumnNamesClause(tableColumns,
               dataChunk.getId(), jobConfig)) // <- SELECT clause
           .append("\n");

      query.append(" FROM ").append(table.toString()).append(" ");

      if (consistentRead) {
        query.append("AS OF SCN ").append(consistentReadScn).append(" ");
      }

      query.append(getPartitionClauseForDataChunk(this.dbInputSplit, idx))
          .append(" t").append("\n");

      query.append(" WHERE (").append(
          getWhereClauseForDataChunk(this.dbInputSplit, idx)).append(")\n");

      // If the user wants the WHERE clause applied to each data-chunk...
      if (whereClauseLocation == OracleUtilities.
              OracleTableImportWhereClauseLocation.SUBSPLIT) {
        String conditions = jobConfig.conditions;
        if (conditions != null && conditions.length() > 0) {
          query.append(" AND (").append(conditions).append(")\n");
        }
      }

    }

    // If the user wants the WHERE clause applied to the whole split...
    if (whereClauseLocation == OracleUtilities.
            OracleTableImportWhereClauseLocation.SPLIT) {
      String conditions = jobConfig.conditions;
      if (conditions != null && conditions.length() > 0) {

        // Insert a "select everything" line at the start of the SQL query...
        query.insert(0, getColumnNamesClause(tableColumns, null, jobConfig) +
            " FROM (\n");

        // ...and then apply the WHERE clause to all the UNIONed sub-queries...
        query.append(")\n").append("WHERE\n").append(conditions).append("\n");
      }
    }

    LOG.info("SELECT QUERY = \n" + query.toString());

    return query.toString();
  }

  private String getColumnNamesClause(OracleTableColumns tableColumns,
      String dataChunkId, FromJobConfig jobConfig) {

    StringBuilder result = new StringBuilder();

    result.append("SELECT ");
    result.append(OracleUtilities.getImportHint(jobConfig));

    int firstFieldIndex = 0;
    int lastFieldIndex = tableColumns.size();
    for (int i = firstFieldIndex; i < lastFieldIndex; i++) {
      if (i > firstFieldIndex) {
        result.append(",");
      }

      OracleTableColumn oracleTableColumn = tableColumns.get(i);
      String fieldName = oracleTableColumn.getName();
      if (oracleTableColumn != null) {
        if (oracleTableColumn.getDataType().equals(
            OracleJdbcConnectorConstants.Oracle.URITYPE)) {
          fieldName = String.format("uritype.geturl(%s) %s", fieldName,
              fieldName);
        }
      }

      result.append(fieldName);
    }
    // We need to insert the value of that data_chunk_id now...
    if (dataChunkId != null && !dataChunkId.isEmpty()) {
      String fieldName =
          String.format(",'%s' %s", dataChunkId,
              OracleJdbcConnectorConstants.COLUMN_NAME_DATA_CHUNK_ID);
      result.append(fieldName);
    }
    return result.toString();
  }

  private String getPartitionClauseForDataChunk(OracleJdbcPartition split,
      int dataChunkIndex) {
    OracleDataChunk dataChunk = split.getDataChunks().get(dataChunkIndex);
    return dataChunk.getPartitionClause();
  }

  private String getWhereClauseForDataChunk(OracleJdbcPartition split,
      int dataChunkIndex) {

    OracleDataChunk dataChunk = split.getDataChunks().get(dataChunkIndex);
    return dataChunk.getWhereClause();
  }

}

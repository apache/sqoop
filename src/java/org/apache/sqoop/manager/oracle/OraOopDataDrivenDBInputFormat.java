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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

/**
 * Reads data from Oracle table - data is divided between mappers based on ROWID
 * split.
 *
 * @param <T> Output type of the record reader
 */
public class OraOopDataDrivenDBInputFormat<T extends SqoopRecord> extends
    DataDrivenDBInputFormat<T> implements Configurable {

  public static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopDataDrivenDBInputFormat.class.getName());

  public OraOopDataDrivenDBInputFormat() {
    super();
    OraOopUtilities.checkJavaSecurityEgd();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {

    int desiredNumberOfMappers = getDesiredNumberOfMappers(jobContext);

    // Resolve the Oracle owner and name of the table we're importing...
    OracleTable table = identifyOracleTableFromJobContext(jobContext);
    List<String> partitionList = getPartitionList(jobContext);

    // Get our Oracle connection...
    Connection connection = getConnection();

    List<InputSplit> splits = null;
    try {
      OracleConnectionFactory.initializeOracleConnection(connection, getConf());

      // The number of chunks generated will *not* be a multiple of the number
      // of splits,
      // to ensure that each split doesn't always get data from the start of
      // each data-file...
      int numberOfChunksPerOracleDataFile = (desiredNumberOfMappers * 2) + 1;

      // Get the Oracle data-chunks for the table...
      List<? extends OraOopOracleDataChunk> dataChunks;
      if (OraOopUtilities.getOraOopOracleDataChunkMethod(getConf()).equals(
          OraOopConstants.OraOopOracleDataChunkMethod.PARTITION)) {
        dataChunks =
            OraOopOracleQueries.getOracleDataChunksPartition(connection, table,
                partitionList);
      } else {
        dataChunks =
            OraOopOracleQueries.getOracleDataChunksExtent(jobContext
                .getConfiguration(), connection, table, partitionList,
                numberOfChunksPerOracleDataFile);
      }

      if (dataChunks.size() == 0) {
        String errMsg;
        if (OraOopUtilities.getOraOopOracleDataChunkMethod(getConf()).equals(
            OraOopConstants.OraOopOracleDataChunkMethod.PARTITION)) {
          errMsg =
              String
                  .format(
                      "The table %s does not contain any partitions and you "
                      + "have specified to chunk the table by partitions.",
                      table.getName());
        } else {
          errMsg =
              String.format("The table %s does not contain any data.", table
                  .getName());
        }
        LOG.fatal(errMsg);
        throw new RuntimeException(errMsg);
      } else {
        OraOopConstants.OraOopOracleBlockToSplitAllocationMethod
            blockAllocationMethod = OraOopUtilities
                .getOraOopOracleBlockToSplitAllocationMethod(
                    jobContext.getConfiguration(),
                    OraOopConstants.
                        OraOopOracleBlockToSplitAllocationMethod.ROUNDROBIN);

        // Group the Oracle data-chunks into splits...
        splits =
            groupTableDataChunksIntoSplits(dataChunks, desiredNumberOfMappers,
                blockAllocationMethod);

        String oraoopLocations =
            jobContext.getConfiguration().get("oraoop.locations", "");
        String[] locations = oraoopLocations.split(",");
        for (int idx = 0; idx < locations.length; idx++) {
          if (idx < splits.size()) {
            String location = locations[idx].trim();
            if (!location.isEmpty()) {
              ((OraOopDBInputSplit) splits.get(idx)).setSplitLocation(location);

              LOG.info(String
                  .format("Split[%d] has been assigned location \"%s\".", idx,
                      location));
            }
          }
        }

      }
    } catch (SQLException ex) {
      throw new IOException(ex);
    }

    return splits;
  }

  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {

    // This code is now running on a Datanode in the Hadoop cluster, so we need
    // to enable debug logging in this JVM...
    OraOopUtilities.enableDebugLoggingIfRequired(conf);

    // Retrieve the JDBC URL that should be used by this mapper.
    // We achieve this by modifying the JDBC URL property in the configuration,
    // prior to the
    // OraOopDBRecordReader (or its ancestors) using the configuration to
    // establish a connection
    // to the database - via DBConfiguration.getConnection()...
    OraOopDBInputSplit oraOopSplit = OraOopDBRecordReader.castSplit(split);
    int mapperId = oraOopSplit.getSplitId();
    String mapperJdbcUrlPropertyName =
        OraOopUtilities.getMapperJdbcUrlPropertyName(mapperId, conf);

    // Get this mapper's JDBC URL
    String mapperJdbcUrl = conf.get(mapperJdbcUrlPropertyName, null);
    LOG.debug(String.format("Mapper %d has a JDBC URL of: %s", mapperId,
        mapperJdbcUrl == null ? "<null>" : mapperJdbcUrl));

    DBConfiguration dbConf = getDBConf();

    if (mapperJdbcUrl != null) {
      // Just changing the URL_PROPERTY in the conf object does not work - as
      // dbConf.getConf()
      // seems to refer to a separate instance of the configuration properties.
      // Therefore, we
      // need to update the URL_PROPERTY in dbConf so that we connect to the
      // appropriate instance
      // in the Oracle RAC. To help avoid confusion, we'll also update the
      // URL_PROPERTY in the
      // conf object to match...
      dbConf.getConf().set(DBConfiguration.URL_PROPERTY, mapperJdbcUrl);
      conf.set(DBConfiguration.URL_PROPERTY, mapperJdbcUrl);
    }

    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());

    try {
      // Use Oracle-specific db reader

      // this.getConnection() will return the connection created when the
      // DBInputFormat ancestor
      // was created. This connection will be based on the URL_PROPERTY that was
      // current at that
      // time. We've just changed the URL_PROPERTY (if this is an Oracle RAC)
      // and therefore need
      // to use dbConf.getConnection() so that a new connection is created using
      // the current
      // value of the URL_PROPERTY...

      return new OraOopDBRecordReader<T>(split, inputClass, conf, dbConf
          .getConnection(), dbConf, dbConf.getInputConditions(), dbConf
          .getInputFieldNames(), dbConf.getInputTableName());
    } catch (SQLException ex) {
      throw new IOException(ex);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

  private OracleTable identifyOracleTableFromJobContext(JobContext jobContext) {

    OracleTable result = new OracleTable();

    String dbUserName =
        jobContext.getConfiguration().get(DBConfiguration.USERNAME_PROPERTY);
    String tableName = getDBConf().getInputTableName();

    result =
        OraOopUtilities.decodeOracleTableName(dbUserName, tableName, jobContext
            .getConfiguration());

    return result;
  }

  private int getDesiredNumberOfMappers(JobContext jobContext) {

    int desiredNumberOfMappers =
        jobContext.getConfiguration().getInt(
            OraOopConstants.ORAOOP_DESIRED_NUMBER_OF_MAPPERS, -1);

    int minMappersAcceptedByOraOop =
        OraOopUtilities.getMinNumberOfImportMappersAcceptedByOraOop(jobContext
            .getConfiguration());

    if (desiredNumberOfMappers < minMappersAcceptedByOraOop) {
      LOG.warn(String.format("%s should not be used to perform a sqoop import "
          + "when the number of mappers is %d\n "
          + "i.e. OraOopManagerFactory.accept() should only appect jobs "
          + "where the number of mappers is at least %d",
          OraOopConstants.ORAOOP_PRODUCT_NAME, desiredNumberOfMappers,
          minMappersAcceptedByOraOop));
    }

    return desiredNumberOfMappers;
  }

  private List<String> getPartitionList(JobContext jobContext) {
    LOG.debug(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST
        + " = "
        + jobContext.getConfiguration().get(
            OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST));
    List<String> result =
        OraOopUtilities.splitOracleStringList(jobContext.getConfiguration()
            .get(OraOopConstants.ORAOOP_IMPORT_PARTITION_LIST));
    if (result != null && result.size() > 0) {
      LOG.debug("Partition filter list: " + result.toString());
    }
    return result;
  }

  protected
      List<InputSplit>
      groupTableDataChunksIntoSplits(
          List<? extends OraOopOracleDataChunk> dataChunks,
          int desiredNumberOfSplits,
          OraOopConstants.OraOopOracleBlockToSplitAllocationMethod
              blockAllocationMethod) {

    int numberOfDataChunks = dataChunks.size();
    int actualNumberOfSplits =
        Math.min(numberOfDataChunks, desiredNumberOfSplits);
    int totalNumberOfBlocksInAllDataChunks = 0;
    for (OraOopOracleDataChunk dataChunk : dataChunks) {
      totalNumberOfBlocksInAllDataChunks += dataChunk.getNumberOfBlocks();
    }

    String debugMsg = String.format(
        "The table being imported by sqoop has %d blocks "
      + "that have been divided into %d chunks "
      + "which will be processed in %d splits. "
      + "The chunks will be allocated to the splits using the method : %s",
        totalNumberOfBlocksInAllDataChunks, numberOfDataChunks,
        actualNumberOfSplits, blockAllocationMethod.toString());
    LOG.info(debugMsg);

    List<InputSplit> splits = new ArrayList<InputSplit>(actualNumberOfSplits);

    for (int i = 0; i < actualNumberOfSplits; i++) {
      OraOopDBInputSplit split = new OraOopDBInputSplit();
      split.setSplitId(i);
      split.setTotalNumberOfBlocksInAllSplits(
          totalNumberOfBlocksInAllDataChunks);
      splits.add(split);
    }

    switch (blockAllocationMethod) {

      case RANDOM:
        // Randomize the order of the data chunks and then "fall through" into
        // the ROUNDROBIN block below...
        Collections.shuffle(dataChunks);

        // NB: No "break;" statement here - we're intentionally falling into the
        // ROUNDROBIN block below...

      //$FALL-THROUGH$
      case ROUNDROBIN:
        int idxSplitRoundRobin = 0;
        for (OraOopOracleDataChunk dataChunk : dataChunks) {

          if (idxSplitRoundRobin >= splits.size()) {
            idxSplitRoundRobin = 0;
          }
          OraOopDBInputSplit split =
              (OraOopDBInputSplit) splits.get(idxSplitRoundRobin++);

          split.getDataChunks().add(dataChunk);
        }
        break;

      case SEQUENTIAL:
        double dataChunksPerSplit = dataChunks.size() / (double) splits.size();
        int dataChunksAllocatedToSplits = 0;

        int idxSplitSeq = 0;
        for (OraOopOracleDataChunk dataChunk : dataChunks) {

          OraOopDBInputSplit split
              = (OraOopDBInputSplit) splits.get(idxSplitSeq);
          split.getDataChunks().add(dataChunk);

          dataChunksAllocatedToSplits++;

          if (dataChunksAllocatedToSplits
                  >= (dataChunksPerSplit * (idxSplitSeq + 1))
              && idxSplitSeq < splits.size()) {
            idxSplitSeq++;
          }
        }
        break;

      default:
        throw new RuntimeException("Block allocation method not implemented.");

    }

    if (LOG.isDebugEnabled()) {
      for (int idx = 0; idx < splits.size(); idx++) {
        LOG.debug("\n\t"
            + ((OraOopDBInputSplit) splits.get(idx)).getDebugDetails());
      }
    }

    return splits;
  }

}

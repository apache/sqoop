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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleDataChunk;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

public class OracleJdbcPartitioner extends
    Partitioner<LinkConfiguration, FromJobConfiguration> {

  private static final Logger LOG =
      Logger.getLogger(OracleJdbcPartitioner.class);

  @Override
  public List<Partition> getPartitions(PartitionerContext context,
      LinkConfiguration linkConfiguration,
      FromJobConfiguration jobConfiguration) {
    try {
      Connection connection = OracleConnectionFactory.makeConnection(
          linkConfiguration.connectionConfig);
      OracleTable table = OracleUtilities.decodeOracleTableName(
          linkConfiguration.connectionConfig.username,
          jobConfiguration.fromJobConfig.tableName);

      long desiredNumberOfMappers = context.getMaxPartitions();
      List<String> partitionList = getPartitionList(
          jobConfiguration.fromJobConfig);

      List<Partition> splits = null;
      try {
        OracleConnectionFactory.initializeOracleConnection(connection,
            linkConfiguration.connectionConfig);

        // The number of chunks generated will *not* be a multiple of the number
        // of splits,
        // to ensure that each split doesn't always get data from the start of
        // each data-file...
        long numberOfChunksPerOracleDataFile = (desiredNumberOfMappers * 2) + 1;

        // Get the Oracle data-chunks for the table...
        List<? extends OracleDataChunk> dataChunks;
        if (OracleUtilities.getOraOopOracleDataChunkMethod(
            jobConfiguration.fromJobConfig).equals(
            OracleUtilities.OracleDataChunkMethod.PARTITION)) {
          dataChunks =
              OracleQueries.getOracleDataChunksPartition(connection, table,
                  partitionList);
        } else {
          dataChunks =
              OracleQueries.getOracleDataChunksExtent(connection, table,
                  partitionList, numberOfChunksPerOracleDataFile);
        }

        if (dataChunks.size() == 0) {
          String errMsg;
          if (OracleUtilities.getOraOopOracleDataChunkMethod(
              jobConfiguration.fromJobConfig).equals(
                  OracleUtilities.OracleDataChunkMethod.PARTITION)) {
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
          OracleUtilities.OracleBlockToSplitAllocationMethod
              blockAllocationMethod = OracleUtilities
                  .getOracleBlockToSplitAllocationMethod(
                      jobConfiguration.fromJobConfig);

          // Group the Oracle data-chunks into splits...
          splits =
              groupTableDataChunksIntoSplits(dataChunks, desiredNumberOfMappers,
                  blockAllocationMethod);

          /*String oraoopLocations =
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
          }*/

        }
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }

      return splits;
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\nError:%s",
          linkConfiguration.connectionConfig.connectionString, ex
              .getMessage()), ex);
    }
  }

  private List<String> getPartitionList(FromJobConfig jobConfig) {
    LOG.debug("Partition list = " + jobConfig.partitionList);
    List<String> result = jobConfig.partitionList;
    if (result != null && result.size() > 0) {
      LOG.debug("Partition filter list: " + result.toString());
    }
    return result;
  }

  protected static
  List<Partition> groupTableDataChunksIntoSplits(
      List<? extends OracleDataChunk> dataChunks,
      long desiredNumberOfSplits,
      OracleUtilities.OracleBlockToSplitAllocationMethod
          blockAllocationMethod) {

  long numberOfDataChunks = dataChunks.size();
  long actualNumberOfSplits =
      Math.min(numberOfDataChunks, desiredNumberOfSplits);
  long totalNumberOfBlocksInAllDataChunks = 0;
  for (OracleDataChunk dataChunk : dataChunks) {
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

  List<Partition> splits =
      new ArrayList<Partition>((int) actualNumberOfSplits);

  for (int i = 0; i < actualNumberOfSplits; i++) {
    OracleJdbcPartition split = new OracleJdbcPartition();
    //split.setSplitId(i);
    //split.setTotalNumberOfBlocksInAllSplits(
    //    totalNumberOfBlocksInAllDataChunks);
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
      for (OracleDataChunk dataChunk : dataChunks) {

        if (idxSplitRoundRobin >= splits.size()) {
          idxSplitRoundRobin = 0;
        }
        OracleJdbcPartition split =
            (OracleJdbcPartition) splits.get(idxSplitRoundRobin++);

        split.getDataChunks().add(dataChunk);
      }
      break;

    case SEQUENTIAL:
      double dataChunksPerSplit = dataChunks.size() / (double) splits.size();
      int dataChunksAllocatedToSplits = 0;

      int idxSplitSeq = 0;
      for (OracleDataChunk dataChunk : dataChunks) {

        OracleJdbcPartition split =
            (OracleJdbcPartition) splits.get(idxSplitSeq);
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
          + splits.get(idx).toString());
    }
  }

  return splits;
  }

}

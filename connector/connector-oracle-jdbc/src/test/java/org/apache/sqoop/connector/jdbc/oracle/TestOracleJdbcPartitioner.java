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

import java.util.ArrayList;
import java.util.List;

import org.apache.sqoop.connector.jdbc.oracle.util.OracleDataChunk;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleDataChunkExtent;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.OracleBlockToSplitAllocationMethod;
import org.apache.sqoop.job.etl.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for OracleJdbcPartitioner.
 */
public class TestOracleJdbcPartitioner {

  @Test
  public void testgroupTableDataChunksIntoSplits() {

    List<OracleDataChunk> dataChunks =
        new ArrayList<OracleDataChunk>();

    int startBlockNumber = 1;
    for (int idx = 0; idx < 241; idx++) {
      OracleDataChunk dataChunk =
          new OracleDataChunkExtent("23480", 666, 1, startBlockNumber,
              startBlockNumber + 8);
      startBlockNumber += 8;
      dataChunks.add(dataChunk);
    }

    List<Partition> splits =
        OracleJdbcPartitioner.groupTableDataChunksIntoSplits(dataChunks, 32,
            OracleBlockToSplitAllocationMethod.SEQUENTIAL);

    int highestNumberOfDataChunksAllocatedToASplit = 0;
    int lowestNumberOfDataChunksAllocatedToASplit = Integer.MAX_VALUE;

    // Check that all splits have data-chunks assigned to them...
    for (Partition split : splits) {
      int dataChunksAllocatedToThisSplit =
          ((OracleJdbcPartition) split).getNumberOfDataChunks();
      highestNumberOfDataChunksAllocatedToASplit =
          Math.max(highestNumberOfDataChunksAllocatedToASplit,
              dataChunksAllocatedToThisSplit);
      lowestNumberOfDataChunksAllocatedToASplit =
          Math.min(lowestNumberOfDataChunksAllocatedToASplit,
              dataChunksAllocatedToThisSplit);
    }

    if (lowestNumberOfDataChunksAllocatedToASplit == 0) {
      Assert
          .fail("There is a split that has not had any "
              + "data-chunks allocated to it.");
    }

    // Check that the split with the least data-chunks has at least
    // 75% of the number of data-chunks of the split with the most
    // data-chunks...
    double minExpectedWorkloadRatio = 0.75;
    double actualWorkloadRatio =
        (double) lowestNumberOfDataChunksAllocatedToASplit
            / highestNumberOfDataChunksAllocatedToASplit;
    if (actualWorkloadRatio < minExpectedWorkloadRatio) {
      Assert.fail(String.format(
          "There is too much difference in the amount of work assigned "
              + "to the 'smallest' split and the 'largest' split. "
              + "The split with the least work should have at least %s "
              + "of the workload of the 'largest' split, but it actually "
              + "only has %s of the workload of the 'largest' split.",
          minExpectedWorkloadRatio, actualWorkloadRatio));
    }
  }

  @Test
  public void testLongBlockId() {
    OracleDataChunkExtent chunk =
        new OracleDataChunkExtent("1", 100, 1, 2147483648L, 4294967295L);
    String whereClause = chunk.getWhereClause();
    Assert.assertNotNull(whereClause);
  }

}

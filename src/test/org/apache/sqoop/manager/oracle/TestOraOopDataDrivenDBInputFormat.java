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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.sqoop.lib.SqoopRecord;

import org.apache.sqoop.manager.oracle.OraOopConstants.
           OraOopOracleBlockToSplitAllocationMethod;

/**
 * Unit tests for OraOopDataDrivenDBInputFormat.
 */
public class TestOraOopDataDrivenDBInputFormat extends OraOopTestCase {

  /**
   * We're just exposing a protected method so that it can be called by this
   * unit test...
   */
  public class Exposer<T extends SqoopRecord> extends
      OraOopDataDrivenDBInputFormat<T> {

    @Override
    public
        List<InputSplit>
        groupTableDataChunksIntoSplits(
            List<? extends OraOopOracleDataChunk> dataChunks,
            int desiredNumberOfSplits,
            OraOopConstants.OraOopOracleBlockToSplitAllocationMethod
                blockAllocationMethod) {

      return super.groupTableDataChunksIntoSplits(dataChunks,
          desiredNumberOfSplits, blockAllocationMethod);
    }

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testgroupTableDataChunksIntoSplits() {

    List<OraOopOracleDataChunk> dataChunks =
        new ArrayList<OraOopOracleDataChunk>();

    int startBlockNumber = 1;
    for (int idx = 0; idx < 241; idx++) {
      OraOopOracleDataChunk dataChunk =
          new OraOopOracleDataChunkExtent("23480", 666, 1, startBlockNumber,
              startBlockNumber + 8);
      startBlockNumber += 8;
      dataChunks.add(dataChunk);
    }

    @SuppressWarnings("rawtypes")
    Exposer e = new Exposer();

    // Prevent setJdbcFetchSize() from logging information about the fetch-size
    // changing. Otherwise, the junit output will be polluted with messages
    // about
    // things that aren't actually a problem...
    boolean logIsBeingCached = Exposer.LOG.getCacheLogEntries();
    Exposer.LOG.setCacheLogEntries(true);

    List<InputSplit> splits =
        e.groupTableDataChunksIntoSplits(dataChunks, 32,
            OraOopOracleBlockToSplitAllocationMethod.SEQUENTIAL);

    Exposer.LOG.setCacheLogEntries(logIsBeingCached);

    int highestNumberOfDataChunksAllocatedToASplit = 0;
    int lowestNumberOfDataChunksAllocatedToASplit = Integer.MAX_VALUE;

    // Check that all splits have data-chunks assigned to them...
    for (InputSplit split : splits) {
      int dataChunksAllocatedToThisSplit =
          ((OraOopDBInputSplit) split).getNumberOfDataChunks();
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

}

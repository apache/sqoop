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

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class TestOraOopDBInputSplitGetDebugDetails {
    private OraOopDBInputSplit firstSplit;
    private OraOopDBInputSplit secondSplit;
    private String firstSplitResult;

    @Before
    public void initialize() {
        List<OraOopOracleDataChunk> dataChunkList = new ArrayList<>();
        OraOopOracleDataChunkExtent firstDataChunkExtent = new OraOopOracleDataChunkExtent("firstExtent",
                666,1, 10500, 10507);
        OraOopOracleDataChunkExtent secondDataChunkExtent = new OraOopOracleDataChunkExtent("secondExtent",
                666,1, 10508, 10515);
        OraOopOracleDataChunkExtent thirdDataChunkExtent = new OraOopOracleDataChunkExtent("thirdExtent",
                666,1, 10516, 10523);
        OraOopOracleDataChunkExtent fourthDataChunkExtent = new OraOopOracleDataChunkExtent("fourthExtent",
                787,2, 11434, 11450);
        OraOopOracleDataChunkPartition firstDataChunkPartition = new OraOopOracleDataChunkPartition("firstPartition",
                true, 14);
        OraOopOracleDataChunkPartition secondDataChunkPartition = new OraOopOracleDataChunkPartition("secondPartition",
                false, 4);
        OraOopOracleDataChunkPartition thirdDataChunkPartition = new OraOopOracleDataChunkPartition("thirdPartition",
                false, 43);
        dataChunkList.addAll(Arrays.asList(firstDataChunkExtent, secondDataChunkExtent, thirdDataChunkExtent,
                fourthDataChunkExtent, firstDataChunkPartition, secondDataChunkPartition, thirdDataChunkPartition));
        firstSplit = new OraOopDBInputSplit(dataChunkList);
        secondSplit = new OraOopDBInputSplit();
        firstSplitResult = "Split[0] includes the Oracle data-chunks:" +
                "\n\t Data chunk info:" +
                "\n\t\t id = firstExtent" +
                "\n\t\t oracleDataObjectId = 666" +
                "\n\t\t relativeDatafileNumber = 1" +
                "\n\t\t startBlockNumber = 10500" +
                "\n\t\t finishBlockNumber = 10507" +
                "\n\t Data chunk info:" +
                "\n\t\t id = secondExtent" +
                "\n\t\t oracleDataObjectId = 666" +
                "\n\t\t relativeDatafileNumber = 1" +
                "\n\t\t startBlockNumber = 10508" +
                "\n\t\t finishBlockNumber = 10515" +
                "\n\t Data chunk info:" +
                "\n\t\t id = thirdExtent" +
                "\n\t\t oracleDataObjectId = 666" +
                "\n\t\t relativeDatafileNumber = 1" +
                "\n\t\t startBlockNumber = 10516" +
                "\n\t\t finishBlockNumber = 10523" +
                "\n\t Data chunk info:" +
                "\n\t\t id = fourthExtent" +
                "\n\t\t oracleDataObjectId = 787" +
                "\n\t\t relativeDatafileNumber = 2" +
                "\n\t\t startBlockNumber = 11434" +
                "\n\t\t finishBlockNumber = 11450" +
                "\n\t Data chunk info:" +
                "\n\t\t id = firstPartition" +
                "\n\t\t isSubPartition = true" +
                "\n\t\t blocks = 14" +
                "\n\t Data chunk info:" +
                "\n\t\t id = secondPartition" +
                "\n\t\t isSubPartition = false" +
                "\n\t\t blocks = 4" +
                "\n\t Data chunk info:" +
                "\n\t\t id = thirdPartition" +
                "\n\t\t isSubPartition = false" +
                "\n\t\t blocks = 43";
    }

    @Test
    public void testGetDebugDetails() {
        assertEquals(firstSplitResult, firstSplit.getDebugDetails());
    }

    @Test
    public void testEmptySplitDebugDetails(){
        assertEquals("Split[-1] does not contain any Oracle data-chunks.", secondSplit.getDebugDetails());
    }
}

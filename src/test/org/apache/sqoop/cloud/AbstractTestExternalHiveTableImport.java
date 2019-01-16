/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.cloud;

import static org.apache.sqoop.tool.BaseSqoopTool.FMT_PARQUETFILE_ARG;
import static org.apache.sqoop.tool.BaseSqoopTool.FMT_TEXTFILE_ARG;
import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.cloud.tools.CloudTestDataSet;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractTestExternalHiveTableImport extends CloudImportJobTestCase {

  private static CloudTestDataSet dataSet = new CloudTestDataSet();

  @Parameterized.Parameters(name = "fileFormatArg = {0}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(new Object[]{FMT_TEXTFILE_ARG, dataSet.getExpectedTextOutputAsList()},
        new Object[]{FMT_PARQUETFILE_ARG, dataSet.getExpectedParquetOutput()});
  }

  public static final Log LOG = LogFactory.getLog(AbstractTestExternalHiveTableImport.class.getName());

  private String fileFormatArg;

  private List<String> expectedResult;

  protected AbstractTestExternalHiveTableImport(CloudCredentialsRule credentialsRule, String fileFormatArg, List<String> expectedResult) {
    super(credentialsRule);
    this.fileFormatArg = fileFormatArg;
    this.expectedResult = expectedResult;
  }

  private HiveMiniCluster hiveMiniCluster;

  private HiveServer2TestUtil hiveServer2TestUtil;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    hiveMiniCluster = createCloudBasedHiveMiniCluster();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  @After
  public void stopMiniCluster() {
    if (hiveMiniCluster != null) {
      hiveMiniCluster.stop();
    }
  }

  @Test
  public void testImportIntoExternalHiveTable() throws IOException {
    String[] args = getExternalHiveTableImportArgs(false);
    runImport(args);

    List<String> rows = hiveServer2TestUtil.loadCsvRowsFromTable(getTableName());
    assertEquals(rows, expectedResult);
  }

  @Test
  public void testCreateAndImportIntoExternalHiveTable() throws IOException {
    String[] args = getExternalHiveTableImportArgs(true);
    runImport(args);

    List<String> rows = hiveServer2TestUtil.loadCsvRowsFromTable(getHiveExternalTableName());
    assertEquals(rows, expectedResult);
  }

  private String[] getExternalHiveTableImportArgs(boolean createHiveTable) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTestsWithFileFormatOption(fileSystemRule.getTargetDirPath().toString(), fileFormatArg);
    builder = addExternalHiveTableImportArgs(builder, hiveMiniCluster.getUrl(), fileSystemRule.getExternalTableDirPath().toString());
    if (createHiveTable) {
      builder = addCreateHiveTableArgs(builder, getHiveExternalTableName());
    }
    return builder.build();
  }
}

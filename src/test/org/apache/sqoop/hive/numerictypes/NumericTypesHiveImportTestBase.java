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

package org.apache.sqoop.hive.numerictypes;

import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.importjob.configuration.HiveTestConfiguration;
import org.apache.sqoop.importjob.numerictypes.NumericTypesImportTestBase;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.NumericTypesTestUtils;

import static java.util.Arrays.deepEquals;
import static org.junit.Assert.assertTrue;

public abstract class NumericTypesHiveImportTestBase<T extends HiveTestConfiguration> extends NumericTypesImportTestBase<T> {

  public NumericTypesHiveImportTestBase(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly,
                                        HiveMiniCluster hiveMiniCluster, HiveServer2TestUtil hiveServer2TestUtil) {
    super(configuration, failWithoutExtraArgs, failWithPaddingOnly);
    this.hiveServer2TestUtil = hiveServer2TestUtil;
    this.hiveMiniCluster = hiveMiniCluster;
  }

  private final HiveMiniCluster hiveMiniCluster;

  private final HiveServer2TestUtil hiveServer2TestUtil;

  @Override
  public ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder()
        .withCommonHadoopFlags()
        .withProperty("parquetjob.configurator.implementation", "hadoop")
        .withOption("connect", getAdapter().getConnectionString())
        .withOption("table", getTableName())
        .withOption("hive-import")
        .withOption("hs2-url", hiveMiniCluster.getUrl())
        .withOption("num-mappers", "1")
        .withOption("as-parquetfile")
        .withOption("delete-target-dir");
    NumericTypesTestUtils.addEnableParquetDecimal(builder);
    return builder;
  }

  @Override
  public void verify() {
    // The result contains a byte[] so we have to use Arrays.deepEquals() to assert.
    Object[] firstRow = hiveServer2TestUtil.loadRawRowsFromTable(getTableName()).iterator().next().toArray();
    Object[] expectedResultsForHive = getConfiguration().getExpectedResultsForHive();
    assertTrue(deepEquals(expectedResultsForHive, firstRow));
  }
}

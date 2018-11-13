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

package org.apache.sqoop.hive;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.KerberosAuthenticationConfiguration;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.apache.sqoop.testcategories.KerberizedTest;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HiveServer2TestUtil;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(KerberizedTest.class)
public class TestHiveServer2TextImport extends ImportJobTestCase {

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  private HiveMiniCluster hiveMiniCluster;

  private HiveServer2TestUtil hiveServer2TestUtil;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    KerberosAuthenticationConfiguration authenticationConfiguration = new KerberosAuthenticationConfiguration(miniKdcInfrastructure);
    hiveMiniCluster = new HiveMiniCluster(authenticationConfiguration);
    hiveMiniCluster.start();
    hiveServer2TestUtil = new HiveServer2TestUtil(hiveMiniCluster.getUrl());
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    hiveMiniCluster.stop();
  }

  @Test
  public void testImport() throws Exception {
    List<Object> columnValues = Arrays.<Object>asList("test", 42, "somestring");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, columnValues);

    String[] args = new ArgumentArrayBuilder()
        .withProperty(YarnConfiguration.RM_PRINCIPAL, miniKdcInfrastructure.getTestPrincipal())
        .withOption("connect", getConnectString())
        .withOption("table", getTableName())
        .withOption("hive-import")
        .withOption("hs2-url", hiveMiniCluster.getUrl())
        .withOption("split-by", getColName(1))
        .withOption("delete-target-dir")
        .build();

    runImport(args);

    List<List<Object>> rows = hiveServer2TestUtil.loadRawRowsFromTable(getTableName());
    assertEquals(columnValues, rows.get(0));
  }
}

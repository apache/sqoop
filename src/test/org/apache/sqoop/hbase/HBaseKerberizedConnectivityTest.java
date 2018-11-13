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

package org.apache.sqoop.hbase;

import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.apache.sqoop.testcategories.KerberizedTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(KerberizedTest.class)
public class HBaseKerberizedConnectivityTest extends HBaseTestCase {

  private static final String HBASE_TABLE_NAME = "KerberosTest";
  private static final String HBASE_COLUMN_FAMILY = "TestColumnFamily";
  private static final String TEST_ROW_KEY = "0";
  private static final String TEST_ROW_VALUE = "1";
  private static final String[] COLUMN_TYPES = { "INT", "INT" };

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  public HBaseKerberizedConnectivityTest() {
    super(miniKdcInfrastructure);
  }

  @Test
  public void testSqoopImportWithKerberizedHBaseConnectivitySucceeds() throws IOException {
    String[] argv = getArgv(true, HBASE_TABLE_NAME, HBASE_COLUMN_FAMILY, true, null);
    createTableWithColTypes(COLUMN_TYPES, new String[] { TEST_ROW_KEY, TEST_ROW_VALUE });

    runImport(argv);

    verifyHBaseCell(HBASE_TABLE_NAME, TEST_ROW_KEY, HBASE_COLUMN_FAMILY, getColName(1), TEST_ROW_VALUE);
  }
}

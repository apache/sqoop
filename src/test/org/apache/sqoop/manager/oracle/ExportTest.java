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

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test exporting data into Oracle.
 */
public class ExportTest extends OraOopTestCase {

  private static final ExportTest TEST_CASE = new ExportTest();

  @BeforeClass
  public static void setUpHdfsData() throws Exception {
    // Copy the TST_PRODUCT table into HDFS which can be used for the export
    // tests
    TEST_CASE.setSqoopTargetDirectory(TEST_CASE.getSqoopTargetDirectory()
        + "tst_product");
    TEST_CASE.createTable("table_tst_product.xml");

    int retCode =
        TEST_CASE.runImport("tst_product", TEST_CASE.getSqoopConf(), false);
    Assert.assertEquals("Return code should be 0", 0, retCode);
  }

  @Test
  public void testProductExport() throws Exception {
    int retCode =
        TEST_CASE.runExportFromTemplateTable("tst_product", "tst_product_exp");
    Assert.assertEquals("Return code should be 0", 0, retCode);
  }

  @Test
  public void testProductExportMixedCaseTableName() throws Exception {
    int retCode =
        TEST_CASE.runExportFromTemplateTable("tst_product",
            "\"\"T5+_Pr#duct_Exp\"\"");
    Assert.assertEquals("Return code should be 0", 0, retCode);
  }

  @AfterClass
  public static void cleanUpHdfsData() throws Exception {
    TEST_CASE.cleanupFolders();
    TEST_CASE.closeTestEnvConnection();
  }

}

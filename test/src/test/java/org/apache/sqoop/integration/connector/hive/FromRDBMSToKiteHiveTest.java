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
package org.apache.sqoop.integration.connector.hive;

import org.apache.sqoop.common.test.asserts.ProviderAsserts;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.HiveConnectorTestCase;
import org.testng.ITest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

@Test(groups = {"slow", "no-real-cluster"})
public class FromRDBMSToKiteHiveTest extends HiveConnectorTestCase implements ITest {
  private String testName;

  private FileFormat fileFormat;

  private MLink rdbmsLink;
  private MLink kiteLink;
  private String hiveTableName;

  @Factory(dataProvider="rdbms-to-kite-hive-test")
  public FromRDBMSToKiteHiveTest(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  @DataProvider(name="rdbms-to-kite-hive-test", parallel=true)
  public static Object[][] data() throws Exception {
    DatabaseProvider provider = DatabaseProviderFactory.getProvider(System.getProperties());
    return new Object[][]{
        {FileFormat.AVRO},
        {FileFormat.PARQUET}
    };
  }

  @Override
  public String getTestName() {
    return testName + "[" + fileFormat.name() + "]";
  }

  public String getHiveTableName() {
    return testName + "_" + fileFormat.name();
  }

  public String getDatasetURI() {
    return "dataset:hive:" + getHiveTableName();
  }

  @BeforeMethod(alwaysRun = true)
  public void setTestName(Method aMethod) {
    this.testName = aMethod.getName();
  }

  @BeforeMethod(alwaysRun = true)
  public void createTable() {
    createAndLoadTableCities();
  }

  @AfterMethod(alwaysRun = true)
  public void dropTable() {
    super.dropTable();
  }

  @BeforeMethod(alwaysRun = true)
  public void createLinks() {
    // RDBMS link
    rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // Kite link
    kiteLink = getClient().createLink("kite-connector");
    kiteLink.getConnectorLinkConfig().getStringInput("linkConfig.authority")
        .setValue(metastoreServerRunner.getAuthority());
    saveLink(kiteLink);
  }

  @Test
  public void testCities() throws Exception {
    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), kiteLink.getName());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");
    List<String> columns = new java.util.LinkedList<>();
    columns.add("id");
    job.getFromJobConfig().getListInput("fromJobConfig.columnList").setValue(columns);

    // Fill the Kite "TO" config
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.uri").setValue(getDatasetURI());
    toConfig.getEnumInput("toJobConfig.fileFormat").setValue(this.fileFormat);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

    saveJob(job);
    executeJob(job);

    // Assert correct output
    ProviderAsserts.assertRow(hiveProvider, new TableName(getHiveTableName()), new Object[]{"id", 1}, "1");
    ProviderAsserts.assertRow(hiveProvider, new TableName(getHiveTableName()), new Object[]{"id", 2}, "2");
    ProviderAsserts.assertRow(hiveProvider, new TableName(getHiveTableName()), new Object[]{"id", 3}, "3");
    ProviderAsserts.assertRow(hiveProvider, new TableName(getHiveTableName()), new Object[]{"id", 4}, "4");

    hiveProvider.dropTable(new TableName(getHiveTableName()));
  }
}

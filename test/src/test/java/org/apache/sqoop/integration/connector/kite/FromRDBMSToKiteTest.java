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
package org.apache.sqoop.integration.connector.kite;

import org.apache.sqoop.connector.common.FileFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class FromRDBMSToKiteTest extends ConnectorTestCase {
  @BeforeMethod(alwaysRun = true)
  public void createTable() {
    createAndLoadTableCities();
  }

  @AfterMethod(alwaysRun = true)
  public void dropTable() {
    super.dropTable();
  }

  /**
   * Kite requires that last directory is dataset name and one to last is namespace.
   *
   * Both names have special requirements ([A-Za-z_][A-Za-z0-9_]*), so we're inserting
   * "namespace" constant in namespace filed, to preserve our (Sqoop integration tests)
   * directory structures.
   */
  @Override
  public String getMapreduceDirectory() {
    return HdfsUtils.joinPathFragments(hadoopCluster.getTestDirectory(), getClass().getName(), "namespace", name).replaceAll("/$", "");
  }

  @Test
  public void testCities() throws Exception {
    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // Kite link
    MLink kiteLink = getClient().createLink("kite-connector");
    kiteLink.getConnectorLinkConfig().getStringInput("linkConfig.authority").setValue(hdfsClient.getUri().getAuthority());
    saveLink(kiteLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getPersistenceId(), kiteLink.getPersistenceId());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");
    job.getFromJobConfig().getStringInput("fromJobConfig.columns").setValue(provider.escapeColumnName("id"));

    // Fill the Kite "TO" config
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.uri").setValue("dataset:hdfs:" + getMapreduceDirectory());
    toConfig.getEnumInput("toJobConfig.fileFormat").setValue(FileFormat.CSV);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

    saveJob(job);
    executeJob(job);

    // Assert correct output
    assertTo(
        "\"1\"",
        "\"2\"",
        "\"3\"",
        "\"4\""
    );
  }
}

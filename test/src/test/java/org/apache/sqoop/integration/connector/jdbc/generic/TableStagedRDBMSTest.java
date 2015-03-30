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
package org.apache.sqoop.integration.connector.jdbc.generic;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.data.Cities;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

/**
 *
 */
public class TableStagedRDBMSTest extends ConnectorTestCase {

  @Test
  public void testStagedTransfer() throws Exception {
    final TableName stageTableName = new TableName("STAGE_" + getTableName());
    createTableCities();
    createFromFile("input-0001",
        "1,'USA','2004-10-23','San Francisco'",
        "2,'USA','2004-10-24','Sunnyvale'",
        "3,'Czech Republic','2004-10-25','Brno'",
        "4,'USA','2004-10-26','Palo Alto'"
      );
    new Cities(provider, stageTableName).createTables();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(hdfsLink.getPersistenceId(),
        rdbmsLink.getPersistenceId());

    // fill HDFS "FROM" config
    fillHdfsFromConfig(job);

    // fill rdbms "TO" config here
    fillRdbmsToConfig(job);
    MConfigList configs = job.getToJobConfig();
    configs.getStringInput("toJobConfig.stageTableName").setValue(provider.escapeTableName(stageTableName.getTableName()));

    // driver config
    MConfigList driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    saveJob(job);

    executeJob(job);

    assertEquals(0L, provider.rowCount(stageTableName));
    assertEquals(4L, provider.rowCount(getTableName()));
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");
    assertRowInCities(2, "USA", "2004-10-24", "Sunnyvale");
    assertRowInCities(3, "Czech Republic", "2004-10-25", "Brno");
    assertRowInCities(4, "USA", "2004-10-26", "Palo Alto");

    // Clean up testing table
    provider.dropTable(stageTableName);
    dropTable();
  }

}

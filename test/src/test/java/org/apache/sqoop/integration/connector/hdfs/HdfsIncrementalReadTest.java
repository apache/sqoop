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
package org.apache.sqoop.integration.connector.hdfs;

import org.apache.sqoop.connector.hdfs.configuration.IncrementalType;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class HdfsIncrementalReadTest extends ConnectorTestCase {

  @BeforeMethod(alwaysRun = true)
  public void createTable() {
    createTableCities();
  }

  @AfterMethod(alwaysRun = true)
  public void dropTable() {
    super.dropTable();
  }

  @Test
  public void testBasic() throws Exception {
    createFromFile("input-0001",
        "1,'USA','2004-10-23','San Francisco'"
    );

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    fillHdfsFromConfig(job);
    job.getFromJobConfig().getEnumInput("incremental.incrementalType").setValue(IncrementalType.NEW_FILES);
    fillRdbmsToConfig(job);
    saveJob(job);

    // Execute for the first time
    executeJob(job);
    assertEquals(provider.rowCount(getTableName()), 1);
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");

    // Second execution
    createFromFile("input-0002",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'"
    );
    executeJob(job);
    assertEquals(provider.rowCount(getTableName()), 3);
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");
    assertRowInCities(2, "USA", "2004-10-24", "Sunnyvale");
    assertRowInCities(3, "Czech Republic", "2004-10-25", "Brno");

    // And last execution
    createFromFile("input-0003",
      "4,'USA','2004-10-26','Palo Alto'"
    );
    executeJob(job);
    assertEquals(provider.rowCount(getTableName()), 4);
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");
    assertRowInCities(2, "USA", "2004-10-24", "Sunnyvale");
    assertRowInCities(3, "Czech Republic", "2004-10-25", "Brno");
    assertRowInCities(4, "USA", "2004-10-26", "Palo Alto");
  }

}

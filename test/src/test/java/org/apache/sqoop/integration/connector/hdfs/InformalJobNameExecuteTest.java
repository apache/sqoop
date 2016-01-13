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

import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.*;

import static org.testng.Assert.assertEquals;

public class InformalJobNameExecuteTest extends ConnectorTestCase {

  private String jobName;

  @Factory(dataProvider="special-job-name-executed-test")
  public InformalJobNameExecuteTest(String specialChar) {
    this.jobName = "job" + specialChar + "name";
  }

  @DataProvider(name="special-job-name-executed-test", parallel=true)
  public static Object[][] data() {
    // The special char used for test. Merge into 3 test cases to reduce the test time.
    return new Object[][] {{" \t/.?&*[]("}, {")`~!@#$%^-"}, {"_=+;:\"<>,"}};
  }

  @BeforeMethod(alwaysRun = true)
  public void createTable() {
    createTableCities();
  }

  @AfterMethod(alwaysRun = true)
  public void dropTable() {
    super.dropTable();
  }

  @Test
  public void test() throws Exception {
    createFromFile("input-0001",
            "1,'USA','2004-10-23','San Francisco'",
            "2,'USA','2004-10-24','Sunnyvale'"
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
    MJob job = getClient().createJob(hdfsLink.getName(), rdbmsLink.getName());

    // set hdfs "FROM" config for the job, since the connector test case base class only has utilities for hdfs!
    fillHdfsFromConfig(job);

    // set the rdbms "TO" config here
    fillRdbmsToConfig(job);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    job.setName(jobName);
    saveJob(job);

    executeJob(job);

    assertEquals(2L, provider.rowCount(getTableName()));
    assertRowInCities(1, "USA", "2004-10-23", "San Francisco");
    assertRowInCities(2, "USA", "2004-10-24", "Sunnyvale");
  }
}

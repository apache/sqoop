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

import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MJob;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Timestamp;

import static org.testng.Assert.assertEquals;

@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class, SqoopInfrastructureProvider.class, DatabaseInfrastructureProvider.class})
public class FromHDFSToRDBMSTest extends SqoopTestCase {
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
      "1,'USA','2004-10-23 00:00:00.000','San Francisco'",
      "2,'USA','2004-10-24 00:00:00.000','Sunnyvale'",
      "3,'Czech Republic','2004-10-25 00:00:00.000','Brno'",
      "4,'USA','2004-10-26 00:00:00.000','Palo Alto'"
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
    saveJob(job);

    executeJob(job);

    assertEquals(4L, provider.rowCount(getTableName()));
    assertRowInCities(1, "USA", Timestamp.valueOf("2004-10-23 00:00:00.000"), "San Francisco");
    assertRowInCities(2, "USA", Timestamp.valueOf("2004-10-24 00:00:00.000"), "Sunnyvale");
    assertRowInCities(3, "Czech Republic", Timestamp.valueOf("2004-10-25 00:00:00.000"), "Brno");
    assertRowInCities(4, "USA", Timestamp.valueOf("2004-10-26 00:00:00.000"), "Palo Alto");
  }
}

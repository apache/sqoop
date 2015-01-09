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

import static org.testng.Assert.assertTrue;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

/**
 * Import simple table with various configurations.
 */
public class FromRDBMSToHDFSTest extends ConnectorTestCase {

  @Test
  public void testBasic() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // srt rdbms "FROM" config
    MConfigList fromConfig = job.getJobConfig(Direction.FROM);
    fromConfig.getStringInput("fromJobConfig.tableName").setValue(provider.escapeTableName(getTableName()));
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue(provider.escapeColumnName("id"));

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
      "1,'USA','San Francisco'",
      "2,'USA','Sunnyvale'",
      "3,'Czech Republic','Brno'",
      "4,'USA','Palo Alto'"
    );

    // Clean up testing table
    dropTable();
  }

  @Test
  public void testColumns() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getPersistenceId(), hdfsLink.getPersistenceId());

    // Connector values
    MConfigList configs = job.getJobConfig(Direction.FROM);
    configs.getStringInput("fromJobConfig.tableName").setValue(provider.escapeTableName(getTableName()));
    configs.getStringInput("fromJobConfig.partitionColumn").setValue(provider.escapeColumnName("id"));
    configs.getStringInput("fromJobConfig.columns").setValue(provider.escapeColumnName("id") + "," + provider.escapeColumnName("country"));
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    saveJob(job);

    MSubmission submission = getClient().startJob(job.getPersistenceId());
    assertTrue(submission.getStatus().isRunning());

    // Wait until the job finish - this active waiting will be removed once
    // Sqoop client API will get blocking support.
    do {
      Thread.sleep(5000);
      submission = getClient().getJobStatus(job.getPersistenceId());
    } while(submission.getStatus().isRunning());

    // Assert correct output
    assertTo(
      "1,'USA'",
      "2,'USA'",
      "3,'Czech Republic'",
      "4,'USA'"
    );

    // Clean up testing table
    dropTable();
  }
}

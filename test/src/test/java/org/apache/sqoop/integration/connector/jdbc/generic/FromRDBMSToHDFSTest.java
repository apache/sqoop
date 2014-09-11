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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Import simple table with various configurations.
 */
public class FromRDBMSToHDFSTest extends ConnectorTestCase {

  private static final Logger LOG = Logger.getLogger(FromRDBMSToHDFSTest.class);

  @Test
  public void testBasic() throws Exception {
    createAndLoadTableCities();

    // RDBMS connection
    MConnection rdbmsConnection = getClient().newConnection("generic-jdbc-connector");
    fillRdbmsConnectionForm(rdbmsConnection);
    createConnection(rdbmsConnection);

    // HDFS connection
    MConnection hdfsConnection = getClient().newConnection("hdfs-connector");
    createConnection(hdfsConnection);

    // Job creation
    MJob job = getClient().newJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Connector values
    MFormList forms = job.getConnectorPart(Direction.FROM);
    forms.getStringInput("fromTable.tableName").setValue(provider.escapeTableName(getTableName()));
    forms.getStringInput("fromTable.partitionColumn").setValue(provider.escapeColumnName("id"));
    fillOutputForm(job, OutputFormat.TEXT_FILE);
    createJob(job);

    runJob(job);

    // Assert correct output
    assertMapreduceOutput(
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

    // RDBMS connection
    MConnection rdbmsConnection = getClient().newConnection("generic-jdbc-connector");
    fillRdbmsConnectionForm(rdbmsConnection);
    createConnection(rdbmsConnection);

    // HDFS connection
    MConnection hdfsConnection = getClient().newConnection("hdfs-connector");
    createConnection(hdfsConnection);

    // Job creation
    MJob job = getClient().newJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Connector values
    MFormList forms = job.getConnectorPart(Direction.FROM);
    forms.getStringInput("fromTable.tableName").setValue(provider.escapeTableName(getTableName()));
    forms.getStringInput("fromTable.partitionColumn").setValue(provider.escapeColumnName("id"));
    forms.getStringInput("fromTable.columns").setValue(provider.escapeColumnName("id") + "," + provider.escapeColumnName("country"));
    fillOutputForm(job, OutputFormat.TEXT_FILE);
    createJob(job);

    MSubmission submission = getClient().startSubmission(job.getPersistenceId());
    assertTrue(submission.getStatus().isRunning());

    // Wait until the job finish - this active waiting will be removed once
    // Sqoop client API will get blocking support.
    do {
      Thread.sleep(5000);
      submission = getClient().getSubmissionStatus(job.getPersistenceId());
    } while(submission.getStatus().isRunning());

    // Assert correct output
    assertMapreduceOutput(
      "1,'USA'",
      "2,'USA'",
      "3,'Czech Republic'",
      "4,'USA'"
    );

    // Clean up testing table
    dropTable();
  }
}

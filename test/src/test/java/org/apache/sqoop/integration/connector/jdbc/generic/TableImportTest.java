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
import org.apache.sqoop.framework.configuration.OutputFormat;
import org.apache.sqoop.framework.configuration.StorageType;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Import simple table with various configurations.
 */
public class TableImportTest extends ConnectorTestCase {

//  private static final Logger LOG = Logger.getLogger(TableImportTest.class);
//
//  @Test
//  public void testBasicImport() throws Exception {
//    createAndLoadTableCities();
//
//    // Connection creation
//    MConnection connection = getClient().newConnection("generic-jdbc-connector");
//    fillConnectionForm(connection);
//    createConnection(connection);
//
//    // Job creation
//    MJob job = getClient().newJob(connection.getPersistenceId(), MJob.Type.IMPORT);
//
//    // Connector values
//    MFormList forms = job.getFromPart();
//    forms.getStringInput("table.tableName").setValue(provider.escapeTableName(getTableName()));
//    forms.getStringInput("table.partitionColumn").setValue(provider.escapeColumnName("id"));
//    // Framework values
//    fillOutputForm(job, StorageType.HDFS, OutputFormat.TEXT_FILE);
//    createJob(job);
//
//    runJob(job);
//
//    // Assert correct output
//    assertMapreduceOutput(
//      "1,'USA','San Francisco'",
//      "2,'USA','Sunnyvale'",
//      "3,'Czech Republic','Brno'",
//      "4,'USA','Palo Alto'"
//    );
//
//    // Clean up testing table
//    dropTable();
//  }
//
//  @Test
//  public void testColumns() throws Exception {
//    createAndLoadTableCities();
//
//    // Connection creation
//    MConnection connection = getClient().newConnection(1L);
//    fillConnectionForm(connection);
//
//    createConnection(connection);
//
//    // Job creation
//    MJob job = getClient().newJob(connection.getPersistenceId(), MJob.Type.IMPORT);
//
//    // Connector values
//    MFormList forms = job.getFromPart();
//    forms.getStringInput("table.tableName").setValue(provider.escapeTableName(getTableName()));
//    forms.getStringInput("table.partitionColumn").setValue(provider.escapeColumnName("id"));
//    forms.getStringInput("table.columns").setValue(provider.escapeColumnName("id") + "," + provider.escapeColumnName("country"));
//    // Framework values
//    fillOutputForm(job, StorageType.HDFS, OutputFormat.TEXT_FILE);
//    createJob(job);
//
//    MSubmission submission = getClient().startSubmission(job.getPersistenceId());
//    assertTrue(submission.getStatus().isRunning());
//
//    // Wait until the job finish - this active waiting will be removed once
//    // Sqoop client API will get blocking support.
//    do {
//      Thread.sleep(5000);
//      submission = getClient().getSubmissionStatus(job.getPersistenceId());
//    } while(submission.getStatus().isRunning());
//
//    // Assert correct output
//    assertMapreduceOutput(
//      "1,'USA'",
//      "2,'USA'",
//      "3,'Czech Republic'",
//      "4,'USA'"
//    );
//
//    // Clean up testing table
//    dropTable();
//  }
}

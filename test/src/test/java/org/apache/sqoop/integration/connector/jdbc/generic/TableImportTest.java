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
import org.apache.sqoop.integration.connector.ConnectorTestCase;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.validation.Status;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Proof of concept implementation of first "real" integration test.
 *
 * Will be improved when client API will be created.
 */
public class TableImportTest extends ConnectorTestCase {

  private static final Logger LOG = Logger.getLogger(TableImportTest.class);

  /**
   * This test is proof of concept.
   *
   * It will be refactored once we will create reasonable client interface.
   */
  @Test
  public void testBasicTableImport() throws Exception {
    createTable("id",
      "id", "int",
      "txt", "varchar(50)"
    );
    insertRow(1, "San Francisco");
    insertRow(2, "Sunnyvale");
    insertRow(3, "Brno");

    // Connection creation and job submission will be refactored once
    // the client API for embedding Sqoop client will be ready.

    // Connection creation
    MConnection connection = getClient().newConnection(1L);

    // Connector values
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(0))).setValue(provider.getJdbcDriver());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(1))).setValue(provider.getConnectionUrl());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(2))).setValue(provider.getConnectionUsername());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(3))).setValue(provider.getConnectionPassword());
    // Framework values
    // No need to set anything

    assertEquals(Status.FINE, getClient().createConnection(connection));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, connection.getPersistenceId());

    // Job creation
    MJob job = getClient().newJob(connection.getPersistenceId(), MJob.Type.IMPORT);

    // Connector values
    ((MStringInput) (job.getConnectorPart().getForms().get(0).getInputs().get(0))).setValue(provider.escapeTableName(getTableName()));
    ((MStringInput) (job.getConnectorPart().getForms().get(0).getInputs().get(3))).setValue(provider.escapeColumnName("id"));
    // Framework values
    ((MEnumInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(0))).setValue(StorageType.HDFS.toString());
    ((MEnumInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(1))).setValue(OutputFormat.TEXT_FILE.toString());
    ((MStringInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(2))).setValue(getMapreduceDirectory());

    assertEquals(Status.FINE, getClient().createJob(job));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, job.getPersistenceId());

    MSubmission submission = getClient().startSubmission(job.getPersistenceId());
    assertTrue(submission.getStatus().isRunning());

    // Wait until the job finish
    do {
      Thread.sleep(5000);
      submission = getClient().getSubmissionStatus(job.getPersistenceId());
    } while(submission.getStatus().isRunning());

    // Assert correct output
    assertMapreduceOutput(
      "1,'San Francisco'",
      "2,'Sunnyvale'",
      "3,'Brno'"
    );

    // Clean up testing table
    dropTable();
  }

}

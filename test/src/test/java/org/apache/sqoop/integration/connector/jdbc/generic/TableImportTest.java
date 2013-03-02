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
import org.apache.sqoop.client.request.ConnectionRequest;
import org.apache.sqoop.client.request.ConnectorRequest;
import org.apache.sqoop.client.request.FrameworkRequest;
import org.apache.sqoop.client.request.JobRequest;
import org.apache.sqoop.client.request.SubmissionRequest;
import org.apache.sqoop.framework.configuration.OutputFormat;
import org.apache.sqoop.framework.configuration.StorageType;
import org.apache.sqoop.integration.connector.ConnectorTestCase;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.json.ValidationBean;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    FrameworkBean frameworkBean = (new FrameworkRequest()).read(getServerUrl());
    ConnectorBean connectorBean = (new ConnectorRequest()).read(getServerUrl(), "1");
    MFramework framework = frameworkBean.getFramework();
    MConnector connector = connectorBean.getConnectors().get(0);
    MConnection connection = new MConnection(connector.getPersistenceId(),
                                             connector.getConnectionForms(),
                                             framework.getConnectionForms());

    // Connector values
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(0))).setValue(provider.getJdbcDriver());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(1))).setValue(provider.getConnectionUrl());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(2))).setValue(provider.getConnectionUsername());
    ((MStringInput) (connection.getConnectorPart().getForms().get(0).getInputs().get(3))).setValue(provider.getConnectionPassword());
    // Framework values
    // No need to set anything

    ValidationBean validationBean = (new ConnectionRequest()).create(getServerUrl(), connection);

    assertEquals(Status.FINE, validationBean.getConnectorValidation().getStatus());
    assertEquals(Status.FINE, validationBean.getFrameworkValidation().getStatus());
    assertNotNull(validationBean.getId());
    connection.setPersistenceId(validationBean.getId());

    // Job creation
    MJob job = new MJob(
      connector.getPersistenceId(),
      connection.getPersistenceId(),
      MJob.Type.IMPORT,
      connector.getJobForms(MJob.Type.IMPORT),
      framework.getJobForms(MJob.Type.IMPORT)
    );

    // Connector values
    ((MStringInput) (job.getConnectorPart().getForms().get(0).getInputs().get(0))).setValue(provider.escapeTableName(getTableName()));
    ((MStringInput) (job.getConnectorPart().getForms().get(0).getInputs().get(3))).setValue(provider.escapeColumnName("id"));
    // Framework values
    ((MEnumInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(0))).setValue(StorageType.HDFS.toString());
    ((MEnumInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(1))).setValue(OutputFormat.TEXT_FILE.toString());
    ((MStringInput) (job.getFrameworkPart().getForms().get(0).getInputs().get(2))).setValue(getMapreduceDirectory());

    validationBean = (new JobRequest()).create(getServerUrl(), job);
    assertEquals(Status.FINE, validationBean.getConnectorValidation().getStatus());
    assertEquals(Status.FINE, validationBean.getFrameworkValidation().getStatus());
    assertNotNull(validationBean.getId());
    job.setPersistenceId(validationBean.getId());

    SubmissionRequest submissionRequest = new SubmissionRequest();

    MSubmission submission = submissionRequest.create(getServerUrl(), "" + job.getPersistenceId()).getSubmission();
    assertTrue(submission.getStatus().isRunning());

    // Wait until the job finish
    do {
      Thread.sleep(5000);
      submission = submissionRequest.read(getServerUrl(), "" + job.getPersistenceId()).getSubmission();
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

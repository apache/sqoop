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
package org.apache.sqoop.integration.server;

import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.OutputFormat;
import org.apache.sqoop.framework.FrameworkError;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Ensure that server will reject starting job when either job itself
 * or corresponding connection is disabled.
 */
@RunWith(Parameterized.class)
public class SubmissionWithDisabledModelObjectsTest extends ConnectorTestCase {

  @Parameterized.Parameters(name = "con({0}) job({1})")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {true, false},
        {false, true},
        {false, false},
    });
  }

  private boolean enabledConnection;
  private boolean enabledJob;

  public SubmissionWithDisabledModelObjectsTest(boolean enabledConnection, boolean enabledJob) {
    this.enabledConnection = enabledConnection;
    this.enabledJob = enabledJob;
  }

  @Test
  public void testWithDisabledObjects() throws Exception {
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
    // Framework values
    fillOutputForm(job, OutputFormat.TEXT_FILE);
    createJob(job);

    // Disable model entities as per parametrized run
    getClient().enableConnection(rdbmsConnection.getPersistenceId(), enabledConnection);
    getClient().enableJob(job.getPersistenceId(), enabledJob);

    // Try to run the job and verify that the it was not executed
    try {
      runJob(job);
      fail("Expected exception as the model classes are disabled.");
    } catch(SqoopException ex) {
      // Top level exception should be CLIENT_0001
      assertEquals(ClientError.CLIENT_0001, ex.getErrorCode());

      // We can directly verify the ErrorCode from SqoopException as client side
      // is not rebuilding SqoopExceptions per missing ErrorCodes. E.g. the cause
      // will be generic Throwable and not SqoopException instance.
      Throwable cause = ex.getCause();
      assertNotNull(cause);

      if(!enabledJob) {
        assertTrue(cause.getMessage().startsWith(FrameworkError.FRAMEWORK_0009.toString()));
      } else if(!enabledConnection) {
        assertTrue(cause.getMessage().startsWith(FrameworkError.FRAMEWORK_0010.toString()));
      } else {
        fail("Unexpected expception retrieved from server " + cause);
      }
    } finally {
      dropTable();
    }
  }
}

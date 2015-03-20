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

import org.apache.hadoop.fs.Path;
import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.error.code.HdfsConnectorError;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 */
public class OutputDirectoryTest extends ConnectorTestCase {
  @Test
  public void testOutputDirectoryIsAFile() throws Exception {
    createAndLoadTableCities();

    hdfsClient.createNewFile(new Path(getMapreduceDirectory()));

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    assertJobSubmissionFailure(job,
      HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0007.toString(),
      "is a file"
    );

    dropTable();
  }

  @Test
  public void testOutputDirectoryIsNotEmpty() throws Exception {
    createAndLoadTableCities();

    hdfsClient.mkdirs(new Path(getMapreduceDirectory()));
    hdfsClient.createNewFile(new Path(getMapreduceDirectory() + "/x"));

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    assertJobSubmissionFailure(job,
      HdfsConnectorError.GENERIC_HDFS_CONNECTOR_0007.toString(),
      "is not empty"
    );

    dropTable();
  }

  @Test
  public void testOutputDirectoryIsEmpty() throws Exception {
    createAndLoadTableCities();

    hdfsClient.mkdirs(new Path(getMapreduceDirectory()));

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);

    saveJob(job);

    executeJob(job);

    // Assert correct output
    assertTo(
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );

    dropTable();
  }

  public void assertJobSubmissionFailure(MJob job, String ...fragments) throws Exception {
    // Try to execute the job and verify that the it was not successful
    try {
      executeJob(job);
      fail("Expected failure in the job submission.");
    } catch (SqoopException ex) {
      // Top level exception should be CLIENT_0001
      assertEquals(ClientError.CLIENT_0001, ex.getErrorCode());

      // We can directly verify the ErrorCode from SqoopException as client side
      // is not rebuilding SqoopExceptions per missing ErrorCodes. E.g. the cause
      // will be generic Throwable and not SqoopException instance.
      Throwable cause = ex.getCause();
      assertNotNull(cause);

      for(String fragment : fragments) {
        assertTrue(cause.getMessage().contains(fragment), "Expected fragment " + fragment + " in error message " + cause.getMessage());
      }
    }
  }
}

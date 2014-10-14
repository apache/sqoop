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
package org.apache.sqoop.repository.derby;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import org.apache.sqoop.common.SqoopException;
import org.junit.Before;
import org.junit.Test;

public class TestRespositorySchemaUpgrade extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    handler = new TestDerbyRepositoryHandler();
  }

  @Test
  public void testHasLatestRepositoryVersion() throws Exception {
    assertFalse(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
    createOrUpgradeSchemaForLatestVersion(); // Test code is building the structures
    assertTrue(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  @Test
  public void testCreateorUpdateRepositorySchema() throws Exception {
    assertFalse(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  // TODO(VB): This should really test for a specific SQL exception that violates the constraints
  @Test(expected=SqoopException.class)
  public void testUpgradeVersion4WithLinkNameAndJobNameDuplicateFailure() throws Exception {
    super.createOrUpgradeSchema(4);
    super.loadConnectorAndDriverConfig(4);
    super.loadConnectionsOrLinks(4);
    super.loadJobs(4);
    // no removing of dupes for job name and link names, hence there should be a exception due to the unique name constraint
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
  }
  // TODO: VB: follow up with the constraint code, which really does not test with examples that has
  // duplicate names, the id list is always of size 1
  //@Test
  public void testUpgradeVersion4WithLinkNameAndJobNameWithNoDuplication() throws Exception {
    super.createOrUpgradeSchema(4);
    super.loadConnectorAndDriverConfig(4);
    super.loadConnectionsOrLinks(4);
    super.loadJobs(4);
    super.removeDuplicateLinkNames(4);
    super.removeDuplicateJobNames(4);
    //  removing duplicate job name and link name, hence there should be no exception with unique name constraint
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  @Test
  public void testUpgradeRepoVersion2ToVersion4() throws Exception {
    super.createOrUpgradeSchema(2);
    assertFalse(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
    loadConnectorAndDriverConfig(2);
    super.loadConnectionsOrLinks(2);
    super.loadJobs(2);
    super.removeDuplicateLinkNames(2);
    super.removeDuplicateJobNames(2);
    // in case of version 2 schema there is no unique job/ link constraint
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRespositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  private class TestDerbyRepositoryHandler extends DerbyRepositoryHandler {
    protected long registerHdfsConnector(Connection conn) {
      try {
        runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
            + "VALUES('hdfs-connector', 'org.apache.sqoop.test.B', '1.0-test')");
        return 2L;
      } catch(Exception e) {
        return -1L;
      }
    }
  }
}
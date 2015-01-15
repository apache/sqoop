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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLIntegrityConstraintViolationException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestRepositoryUpgrade extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();
    handler = new TestDerbyRepositoryHandler();
  }

  @Test
  public void testHasLatestRepositoryVersion() throws Exception {
    assertFalse(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
    createOrUpgradeSchemaForLatestVersion(); // Test code is building the
                                             // structures
    assertTrue(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  @Test
  public void testCreatorUpdateRepositorySchema() throws Exception {
    assertFalse(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  @Test
  public void testDoubleUpdateRepositorySchema() throws Exception {
    // Setup
    assertFalse(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));

    // Exercise and verify
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  @Test(expectedExceptions = SQLIntegrityConstraintViolationException.class)
  public void testUpgradeVersion4WithNonUniqueJobNameFailure() throws Exception {
    super.createOrUpgradeSchema(4);
    // try loading duplicate job names in version 4 and it should throw an
    // exception
    super.loadNonUniqueJobsInVersion4();
  }

  @Test(expectedExceptions = SQLIntegrityConstraintViolationException.class)
  public void testUpgradeVersion4WithNonUniqueLinkNamesAdded() throws Exception {
    super.createOrUpgradeSchema(4);
    // try loading duplicate link names in version 4 and it should throw an
    // exception
    super.loadNonUniqueLinksInVersion4();
  }

  @Test(expectedExceptions = SQLIntegrityConstraintViolationException.class)
  public void testUpgradeVersion4WithNonUniqueConfigurableNamesAdded() throws Exception {
    super.createOrUpgradeSchema(4);
    // try loading duplicate configurable names in version 4 and it should throw
    // an exception
    super.loadNonUniqueConfigurablesInVersion4();
  }

  @Test(expectedExceptions = SQLIntegrityConstraintViolationException.class)
  public void testUpgradeVersion4WithNonUniqueConfigNameAndTypeAdded() throws Exception {
    super.createOrUpgradeSchema(4);
    super.addConnectorB();
    // try loading duplicate config names in version 4 and it should throw an
    // exception
    super.loadNonUniqueConfigNameTypeInVersion4();
  }

  @Test
  public void testUpgradeVersion4WithNonUniqueConfigNameButUniqueTypeAdded() throws Exception {
    super.createOrUpgradeSchema(4);
    super.addConnectorB();
    // try loading duplicate config names but unique type, hence no exception
    super.loadNonUniqueConfigNameButUniqueTypeInVersion4();
  }

  @Test
  public void testUpgradeVersion4WithNonUniqueConfigNameAndTypeButUniqueConfigurable()
      throws Exception {
    super.createOrUpgradeSchema(4);
    super.addConnectorA();
    super.addConnectorB();
    // try loading duplicate config names and type but unique connector, hence
    // no exception
    super.loadNonUniqueConfigNameAndTypeButUniqueConfigurableInVersion4();
  }

  @Test(expectedExceptions = SQLIntegrityConstraintViolationException.class)
  public void testUpgradeVersion4WithNonUniqueInputNameAndTypeAdded() throws Exception {
    super.createOrUpgradeSchema(4);
    super.addConnectorB();
    // try loading duplicate input name and type for a config in version 4 and it should throw an
    // exception
    super.loadNonUniqueInputNameTypeInVersion4();
  }

  @Test
  public void testUpgradeVersion4WithNonUniqueInputNameAndTypeButUniqueConfig()
      throws Exception {
    super.createOrUpgradeSchema(4);
    super.addConnectorA();
    super.addConnectorB();
    // try loading duplicate input names and type but unique config, hence
    // no exception
    super.loadNonUniqueInputNameAndTypeButUniqueConfigInVersion4();
  }

  @Test
  public void testUpgradeRepoVersion2ToVersion4() throws Exception {
    // in case of version 2 schema there is no unique job/ link constraint
    super.createOrUpgradeSchema(2);
    assertFalse(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
    loadConnectorAndDriverConfig(2);
    super.loadConnectionsOrLinks(2);
    super.loadJobs(2);
    handler.createOrUpgradeRepository(getDerbyDatabaseConnection());
    assertTrue(handler.isRepositorySuitableForUse(getDerbyDatabaseConnection()));
  }

  private class TestDerbyRepositoryHandler extends DerbyRepositoryHandler {
    protected long registerHdfsConnector(Connection conn) {
      try {
        TestRepositoryUpgrade.this.runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
            + "VALUES('hdfs-connector', 'org.apache.sqoop.test.B', '1.0-test')");
        return 2L;
      } catch (Exception e) {
        return -1L;
      }
    }
  }
}
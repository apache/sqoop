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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test driver methods on Derby repository.
 */
public class TestDriverHandling extends DerbyTestCase {

  private static final Object CURRENT_DRIVER_VERSION = "1";
  DerbyRepositoryHandler handler;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();
    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    createOrUpgradeSchemaForLatestVersion();
  }

  @Test
  public void testFindDriver() throws Exception {
    // On empty repository, no driverConfig should be there
    assertNull(handler.findDriver(MDriver.DRIVER_NAME, getDerbyDatabaseConnection()));
    // Load Connector and DriverConfig into repository
    // TODO(SQOOP-1582):FIX why load connector config for driver testing?
    // add a connector A and driver SqoopDriver
    loadConnectorAndDriverConfig();
    // Retrieve it
    MDriver driver = handler.findDriver(MDriver.DRIVER_NAME, getDerbyDatabaseConnection());
    assertNotNull(driver);
    assertNotNull(driver.getDriverConfig());
    assertEquals("1.0-test", driver.getVersion());
    assertEquals("1.0-test", driver.getVersion());

    // Get original structure
    MDriverConfig originalDriverConfig = getDriverConfig();
    // And compare them
    assertEquals(originalDriverConfig, driver.getDriverConfig());
  }

  public void testRegisterDriver() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, driver.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONNECTOR", 0);
    assertCountForTable("SQOOP.SQ_CONFIG", 2);
    assertCountForTable("SQOOP.SQ_INPUT", 4);
    assertCountForTable("SQOOP.SQ_INPUT_RELATION", 4);

    // Registered driver and config should be easily recovered back
    MDriver retrieved = handler.findDriver(MDriver.DRIVER_NAME, getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(driver, retrieved);
    assertEquals(driver.getVersion(), retrieved.getVersion());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testRegisterBadDriver() throws Exception {
    MDriver driver = getBadDriver();
    handler.registerDriver(driver, getDerbyDatabaseConnection());
  }

  @Test
  public void testDriverVersionUpgrade() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, getDerbyDatabaseConnection());
    String registeredDriverVersion = handler.findDriver(MDriver.DRIVER_NAME, getDerbyDatabaseConnection()).getVersion();
    assertEquals(CURRENT_DRIVER_VERSION, registeredDriverVersion);
    driver.setVersion("2");
    handler.upgradeDriverAndConfigs(driver, getDerbyDatabaseConnection());
    assertEquals("2", driver.getVersion());
  }
}

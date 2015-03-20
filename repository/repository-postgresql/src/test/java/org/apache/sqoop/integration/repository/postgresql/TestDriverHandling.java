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
package org.apache.sqoop.integration.repository.postgresql;

import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.model.MDriver;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Test driver methods on Derby repository.
 */
@Test(groups = "postgresql")
public class TestDriverHandling extends PostgresqlTestCase {

  private static final Object CURRENT_DRIVER_VERSION = "1";

  @Test
  public void testFindDriver() throws Exception {
    // On empty repository, no driverConfig should be there
    assertNull(handler.findDriver(MDriver.DRIVER_NAME, provider.getConnection()));

    // Register driver
    handler.registerDriver(getDriver(), provider.getConnection());

    // Retrieve it
    MDriver driver = handler.findDriver(MDriver.DRIVER_NAME, provider.getConnection());
    assertNotNull(driver);
    assertNotNull(driver.getDriverConfig());
    assertEquals("1", driver.getVersion());
    assertEquals("1", driver.getVersion());

    // Compare with original
    assertEquals(getDriver().getDriverConfig(), driver.getDriverConfig());
  }

  @Test
  public void testRegisterDriver() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, provider.getConnection());

    // Connector should get persistence ID
    assertEquals(1, driver.getPersistenceId());

    // Now check content in corresponding tables
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIGURABLE")), 1);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIG")), 2);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT")), 4);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT_RELATION")), 3);

    // Registered driver and config should be easily recovered back
    MDriver retrieved = handler.findDriver(MDriver.DRIVER_NAME, provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(driver, retrieved);
    assertEquals(driver.getVersion(), retrieved.getVersion());
  }


  @Test
  public void testDriverVersionUpgrade() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, provider.getConnection());
    String registeredDriverVersion = handler.findDriver(MDriver.DRIVER_NAME, provider.getConnection()).getVersion();
    assertEquals(CURRENT_DRIVER_VERSION, registeredDriverVersion);
    driver.setVersion("2");
    handler.upgradeDriverAndConfigs(driver, provider.getConnection());
    assertEquals("2", driver.getVersion());
  }
}

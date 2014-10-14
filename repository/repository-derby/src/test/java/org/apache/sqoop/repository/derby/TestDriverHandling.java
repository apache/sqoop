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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.junit.Before;
import org.junit.Test;

/**
 * Test driver methods on Derby repository.
 */
public class TestDriverHandling extends DerbyTestCase {

  private static final Object CURRENT_DRIVER_VERSION = "1";
  DerbyRepositoryHandler handler;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    createOrUpgradeSchemaForLatestVersion();
  }

  @Test
  public void testFindDriverConfig() throws Exception {
    // On empty repository, no driverConfig should be there
    assertNull(handler.findDriver(getDerbyDatabaseConnection()));
    // Load Connector and DriverConfig into repository
    // TODO(SQOOP-1582):FIX why load connector config for driver testing?
    loadConnectorAndDriverConfig();
    // Retrieve it
    MDriver driver = handler.findDriver(getDerbyDatabaseConnection());
    assertNotNull(driver);

    // Get original structure
    MDriverConfig originalDriverConfig = getDriverConfig();
    // And compare them
    assertEquals(originalDriverConfig, driver.getDriverConfig());
  }

  public void testRegisterDriverAndConnectorConfig() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, driver.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONNECTOR", 0);
    assertCountForTable("SQOOP.SQ_CONFIG", 2);
    assertCountForTable("SQOOP.SQ_INPUT", 4);

    // Registered driver config should be easily recovered back
    MDriver retrieved = handler.findDriver(getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(driver, retrieved);
    assertEquals(driver.getVersion(), retrieved.getVersion());
  }

  private String getDriverVersion() throws Exception {
    final String frameworkVersionQuery =
      "SELECT SQM_VALUE FROM SQOOP.SQ_SYSTEM WHERE SQM_KEY=?";
    String retVal = null;
    PreparedStatement preparedStmt = null;
    ResultSet resultSet = null;
    try {
      preparedStmt =
        getDerbyDatabaseConnection().prepareStatement(frameworkVersionQuery);
      preparedStmt.setString(1, DerbyRepoConstants.SYSKEY_DRIVER_CONFIG_VERSION);
      resultSet = preparedStmt.executeQuery();
      if(resultSet.next())
        retVal = resultSet.getString(1);
      return retVal;
    } finally {
      if(preparedStmt !=null) {
        try {
          preparedStmt.close();
        } catch(SQLException e) {
        }
      }
      if(resultSet != null) {
        try {
          resultSet.close();
        } catch(SQLException e) {
        }
      }
    }
  }

  @Test
  public void testDriverVersion() throws Exception {
    MDriver driver = getDriver();
    handler.registerDriver(driver, getDerbyDatabaseConnection());

    final String lowerVersion = Integer.toString(Integer
        .parseInt(DriverBean.CURRENT_DRIVER_VERSION) - 1);
    assertEquals(CURRENT_DRIVER_VERSION, getDriverVersion());
    runQuery("UPDATE SQOOP.SQ_SYSTEM SET SQM_VALUE='" + lowerVersion + "' WHERE SQM_KEY = '"
        + DerbyRepoConstants.SYSKEY_DRIVER_CONFIG_VERSION + "'");
    assertEquals(lowerVersion, getDriverVersion());

    handler.upgradeDriver(driver, getDerbyDatabaseConnection());

    assertEquals(CURRENT_DRIVER_VERSION, driver.getVersion());

    assertEquals(CURRENT_DRIVER_VERSION, getDriverVersion());
  }
}

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

import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.model.MDriverConfig;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test driver config methods on Derby repository.
 */
public class TestDriverConfigHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();
  }

  public void testFindDriverConfig() throws Exception {
    // On empty repository, no driverConfig should be there
    assertNull(handler.findDriverConfig(getDerbyDatabaseConnection()));
    // Load Connector and DriverConfig into repository
    loadConnectorAndDriverConfig();
    // Retrieve it
    MDriverConfig driverConfig = handler.findDriverConfig(getDerbyDatabaseConnection());
    assertNotNull(driverConfig);

    // Get original structure
    MDriverConfig originalDriverConfig = getDriverConfig();

    // And compare them
    assertEquals(originalDriverConfig, driverConfig);
  }

  public void testRegisterConnector() throws Exception {
    MDriverConfig driverConfig = getDriverConfig();
    handler.registerDriverConfig(driverConfig, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, driverConfig.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONNECTOR", 0);
    assertCountForTable("SQOOP.SQ_FORM", 4);
    assertCountForTable("SQOOP.SQ_INPUT", 8);

    // Registered framework should be easily recovered back
    MDriverConfig retrieved = handler.findDriverConfig(getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(driverConfig, retrieved);
    assertEquals(driverConfig.getVersion(), retrieved.getVersion());
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
      preparedStmt.setString(1, DerbyRepoConstants.SYSKEY_DRIVER_VERSION);
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

  public void testDriverVersion() throws Exception {
    handler.registerDriverConfig(getDriverConfig(), getDerbyDatabaseConnection());

    final String lowerVersion = Integer.toString(
      Integer.parseInt(Driver.CURRENT_DRIVER_VERSION) - 1);
    assertEquals(Driver.CURRENT_DRIVER_VERSION, getDriverVersion());
    runQuery("UPDATE SQOOP.SQ_SYSTEM SET SQM_VALUE='" + lowerVersion +
      "' WHERE SQM_KEY = '" + DerbyRepoConstants.SYSKEY_DRIVER_VERSION + "'");
    assertEquals(lowerVersion, getDriverVersion());

    MDriverConfig framework = getDriverConfig();
    handler.updateDriverConfig(framework, getDerbyDatabaseConnection());

    assertEquals(Driver.CURRENT_DRIVER_VERSION, framework.getVersion());

    assertEquals(Driver.CURRENT_DRIVER_VERSION, getDriverVersion());
  }
}

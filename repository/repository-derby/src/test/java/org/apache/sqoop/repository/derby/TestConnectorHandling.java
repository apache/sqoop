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

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnector;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Test connector methods on Derby repository.
 */
public class TestConnectorHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();
    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    createOrUpgradeSchemaForLatestVersion();
  }

  @Test
  public void testFindConnector() throws Exception {
    // On empty repository, no connectors should be there
    assertNull(handler.findConnector("A", getDerbyDatabaseConnection()));
    // Load connector into repository
    addConnectorA();
    // Retrieve it
    MConnector connector = handler.findConnector("A", getDerbyDatabaseConnection());
    assertNotNull(connector);
    // Get original structure
    MConnector original = getConnector();
    // And compare them
    assertEquals(original, connector);
  }

  @Test
  public void testFindAllConnectors() throws Exception {
    // No connectors in an empty repository, we expect an empty list
    assertEquals(handler.findConnectors(getDerbyDatabaseConnection()).size(), 0);
    // add connector A
    loadConnectorAndDriverConfig();
    // adding connector B
    addConnectorB();
    // Retrieve connectors
    List<MConnector> connectors = handler.findConnectors(getDerbyDatabaseConnection());
    assertNotNull(connectors);
    assertEquals(connectors.size(), 2);
    assertEquals(connectors.get(0).getUniqueName(), "A");
    assertEquals(connectors.get(1).getUniqueName(), "B");
  }

  @Test
  public void testRegisterConnector() throws Exception {
    MConnector connector = getConnector();
    handler.registerConnector(connector, getDerbyDatabaseConnection());
    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONFIGURABLE", 1);
    assertCountForTable("SQOOP.SQ_CONFIG", 6);
    assertCountForTable("SQOOP.SQ_INPUT", 12);
    assertCountForTable("SQOOP.SQ_INPUT_RELATION", 9);


    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testRegisterConnectorWithIncorrectInputOverridesAttribute() throws Exception {
    MConnector connector = getConnectorWithIncorrectOverridesAttribute();
    handler.registerConnector(connector, getDerbyDatabaseConnection());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testRegisterConnectorWithIncorrectInputOverridesAttribute2() throws Exception {
    MConnector connector = getConnectorWithIncorrectOverridesAttribute2();
    handler.registerConnector(connector, getDerbyDatabaseConnection());
  }

  @Test
  public void testRegisterConnectorWithMultipleInputOverridesAttribute() throws Exception {
    MConnector connector = getConnectorWithMultipleOverridesAttribute();
    handler.registerConnector(connector, getDerbyDatabaseConnection());
  }

  @Test
  public void testFromDirection() throws Exception {
    MConnector connector = getConnector(true, false);
    handler.registerConnector(connector, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONFIGURABLE", 1);
    assertCountForTable("SQOOP.SQ_CONFIG", 4);
    assertCountForTable("SQOOP.SQ_INPUT", 8);
    assertCountForTable("SQOOP.SQ_INPUT_RELATION", 6);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test
  public void testToDirection() throws Exception {
    MConnector connector = getConnector(false, true);

    handler.registerConnector(connector, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONFIGURABLE", 1);
    assertCountForTable("SQOOP.SQ_CONFIG", 4);
    assertCountForTable("SQOOP.SQ_INPUT", 8);
    assertCountForTable("SQOOP.SQ_INPUT_RELATION", 6);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test
  public void testNeitherDirection() throws Exception {
    MConnector connector = getConnector(false, false);

    handler.registerConnector(connector, getDerbyDatabaseConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONFIGURABLE", 1);
    assertCountForTable("SQOOP.SQ_CONFIG", 2);
    assertCountForTable("SQOOP.SQ_INPUT", 4);
    assertCountForTable("SQOOP.SQ_INPUT_RELATION", 3);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }
}

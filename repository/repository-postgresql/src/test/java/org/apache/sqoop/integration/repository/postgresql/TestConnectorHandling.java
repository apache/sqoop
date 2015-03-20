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
import org.apache.sqoop.model.MConnector;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test connector methods on Derby repository.
 */
@Test(groups = "postgresql")
public class TestConnectorHandling extends PostgresqlTestCase {

  @Test
  public void testFindConnector() throws Exception {
    // On empty repository, no connectors should be there
    assertNull(handler.findConnector("A", provider.getConnection()));

    // Register a single connector
    handler.registerConnector(
        getConnector("A", "org.apache.sqoop.test.A", "1.0-test", true, true),
        provider.getConnection());

    // Retrieve it and compare with original
    MConnector connector = handler.findConnector("A", provider.getConnection());
    assertNotNull(connector);
    assertEquals(
        getConnector("A", "org.apache.sqoop.test.A", "1.0-test", true, true),
        connector);
  }

  @Test
  public void testFindAllConnectors() throws Exception {
    // No connectors in an empty repository, we expect an empty list
    assertEquals(handler.findConnectors(provider.getConnection()).size(), 0);

    // Register connectors
    handler.registerConnector(
        getConnector("A", "org.apache.sqoop.test.A", "1.0-test", true, true),
        provider.getConnection());
    handler.registerConnector(
        getConnector("B", "org.apache.sqoop.test.B", "1.0-test", true, true),
        provider.getConnection());

//    loadConfigurables();
    // Retrieve connectors
    List<MConnector> connectors = handler.findConnectors(provider.getConnection());
    assertNotNull(connectors);
    assertEquals(connectors.size(), 2);
    assertEquals(connectors.get(0).getUniqueName(), "A");
    assertEquals(connectors.get(1).getUniqueName(), "B");
  }

  @Test
  public void testRegisterConnector() throws Exception {
    MConnector connector = getConnector("A", "org.apache.sqoop.test.A", "1.0-test", true, true);
    handler.registerConnector(connector, provider.getConnection());
    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIGURABLE")), 1);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIG")), 6);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT")), 12);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT_RELATION")), 9);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test
  public void testFromDirection() throws Exception {
    MConnector connector = getConnector("A", "org.apache.sqoop.test.A", "1.0-test", true, false);

    handler.registerConnector(connector, provider.getConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIGURABLE")), 1);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIG")), 4);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT")), 8);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT_RELATION")), 6);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test
  public void testToDirection() throws Exception {
    MConnector connector = getConnector("A", "org.apache.sqoop.test.A", "1.0-test", false, true);

    handler.registerConnector(connector, provider.getConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIGURABLE")), 1);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIG")), 4);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT")), 8);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT_RELATION")), 6);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }

  @Test
  public void testNeitherDirection() throws Exception {
    MConnector connector = getConnector("A", "org.apache.sqoop.test.A", "1.0-test", false, false);

    handler.registerConnector(connector, provider.getConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIGURABLE")), 1);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_CONFIG")), 2);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT")), 4);
    assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_INPUT_RELATION")), 3);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }
}

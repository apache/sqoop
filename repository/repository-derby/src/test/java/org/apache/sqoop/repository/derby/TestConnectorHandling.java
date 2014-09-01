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

import org.apache.sqoop.model.MConnector;

import java.util.List;

/**
 * Test connector methods on Derby repository.
 */
public class TestConnectorHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();
  }

  public void testFindConnector() throws Exception {
    // On empty repository, no connectors should be there
    assertNull(handler.findConnector("A", getDerbyConnection()));
    assertNull(handler.findConnector("B", getDerbyConnection()));

    // Load connector into repository
    loadConnectorAndFramework();

    // Retrieve it
    MConnector connector = handler.findConnector("A", getDerbyConnection());
    assertNotNull(connector);

    // Get original structure
    MConnector original = getConnector();

    // And compare them
    assertEquals(original, connector);
  }

  public void testFindAllConnectors() throws Exception {
    // No connectors in an empty repository, we expect an empty list
    assertEquals(handler.findConnectors(getDerbyConnection()).size(),0);

    loadConnectorAndFramework();
    addConnector();

    // Retrieve connectors
    List<MConnector> connectors = handler.findConnectors(getDerbyConnection());
    assertNotNull(connectors);
    assertEquals(connectors.size(),2);
    assertEquals(connectors.get(0).getUniqueName(),"A");
    assertEquals(connectors.get(1).getUniqueName(),"B");


  }

  public void testRegisterConnector() throws Exception {
    MConnector connector = getConnector();

    handler.registerConnector(connector, getDerbyConnection());

    // Connector should get persistence ID
    assertEquals(1, connector.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONNECTOR", 1);
    assertCountForTable("SQOOP.SQ_FORM", 6);
    assertCountForTable("SQOOP.SQ_INPUT", 12);

    // Registered connector should be easily recovered back
    MConnector retrieved = handler.findConnector("A", getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(connector, retrieved);
  }
}

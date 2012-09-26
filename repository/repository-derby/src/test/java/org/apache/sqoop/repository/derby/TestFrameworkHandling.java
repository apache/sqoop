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

import org.apache.sqoop.model.MFramework;

/**
 * Test framework methods on Derby repository.
 */
public class TestFrameworkHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();
  }

  public void testFindFramework() throws Exception {
    // On empty repository, no framework should be there
    assertNull(handler.findFramework(getDerbyConnection()));

    // Load framework into repository
    loadConnectorAndFramework();

    // Retrieve it
    MFramework framework = handler.findFramework(getDerbyConnection());
    assertNotNull(framework);

    // Get original structure
    MFramework original = getFramework();

    // And compare them
    assertEquals(original, framework);
  }

  public void testRegisterConnector() throws Exception {
    MFramework framework = getFramework();

    handler.registerFramework(framework, getDerbyConnection());

    // Connector should get persistence ID
    assertEquals(1, framework.getPersistenceId());

    // Now check content in corresponding tables
    assertCountForTable("SQOOP.SQ_CONNECTOR", 0);
    assertCountForTable("SQOOP.SQ_FORM", 6);
    assertCountForTable("SQOOP.SQ_INPUT", 12);

    // Registered framework should be easily recovered back
    MFramework retrieved = handler.findFramework(getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(framework, retrieved);
  }
}

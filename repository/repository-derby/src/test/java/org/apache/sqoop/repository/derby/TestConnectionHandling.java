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
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test connection methods on Derby repository.
 */
public class TestConnectionHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();

    // We always needs connector and framework structures in place
    loadConnectorAndFramework();
  }

  public void testFindConnection() throws Exception {
    // Let's try to find non existing connection
    try {
      handler.findConnection(1, getDerbyConnection());
      fail();
    } catch(SqoopException ex) {
      assertEquals(DerbyRepoError.DERBYREPO_0024, ex.getErrorCode());
    }

    // Load prepared connections into database
    loadConnections();

    MConnection connA = handler.findConnection(1, getDerbyConnection());
    assertNotNull(connA);
    assertEquals(1, connA.getPersistenceId());
    assertEquals("CA", connA.getName());

    List<MForm> forms;

    // Check connector part
    forms = connA.getConnectorPart().getForms();
    assertEquals("Value1", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value3", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    // Check framework part
    forms = connA.getFrameworkPart().getForms();
    assertEquals("Value13", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());
  }

  public void testFindConnections() throws Exception {
    List<MConnection> list;

    // Load empty list on empty repository
    list = handler.findConnections(getDerbyConnection());
    assertEquals(0, list.size());

    loadConnections();

    // Load all two connections on loaded repository
    list = handler.findConnections(getDerbyConnection());
    assertEquals(2, list.size());

    assertEquals("CA", list.get(0).getName());
    assertEquals("CB", list.get(1).getName());
  }

  public void testExistsConnection() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsConnection(1, getDerbyConnection()));
    assertFalse(handler.existsConnection(2, getDerbyConnection()));
    assertFalse(handler.existsConnection(3, getDerbyConnection()));

    loadConnections();

    assertTrue(handler.existsConnection(1, getDerbyConnection()));
    assertTrue(handler.existsConnection(2, getDerbyConnection()));
    assertFalse(handler.existsConnection(3, getDerbyConnection()));
  }

  public void testCreateConnection() throws Exception {
    MConnection connection = getConnection();

    // Load some data
    fillConnection(connection);

    handler.createConnection(connection, getDerbyConnection());

    assertEquals(1, connection.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 1);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 4);

    MConnection retrieved = handler.findConnection(1, getDerbyConnection());
    assertEquals(1, retrieved.getPersistenceId());

    List<MForm> forms;
    forms = connection.getConnectorPart().getForms();
    assertEquals("Value1", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    forms = connection.getFrameworkPart().getForms();
    assertEquals("Value13", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    // Let's create second connection
    connection = getConnection();
    fillConnection(connection);

    handler.createConnection(connection, getDerbyConnection());

    assertEquals(2, connection.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 2);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 8);
  }

  public void testInUseConnection() throws Exception {
    loadConnections();

    assertFalse(handler.inUseConnection(1, getDerbyConnection()));

    loadJobs();

    assertTrue(handler.inUseConnection(1, getDerbyConnection()));
  }

  public void testUpdateConnection() throws Exception {
    loadConnections();

    MConnection connection = handler.findConnection(1, getDerbyConnection());

    List<MForm> forms;

    forms = connection.getConnectorPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(null);
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(1).getInputs().get(1)).setValue(null);

    forms = connection.getFrameworkPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    connection.setName("name");

    handler.updateConnection(connection, getDerbyConnection());

    assertEquals(1, connection.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 2);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 10);

    MConnection retrieved = handler.findConnection(1, getDerbyConnection());
    assertEquals("name", connection.getName());

    forms = retrieved.getConnectorPart().getForms();
    assertEquals("Updated", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Updated", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    forms = retrieved.getFrameworkPart().getForms();
    assertEquals("Updated", forms.get(0).getInputs().get(0).getValue());
    assertNotNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals(((Map)forms.get(0).getInputs().get(1).getValue()).size(), 0);
    assertEquals("Updated", forms.get(1).getInputs().get(0).getValue());
    assertNotNull(forms.get(1).getInputs().get(1).getValue());
    assertEquals(((Map)forms.get(1).getInputs().get(1).getValue()).size(), 0);
  }

  public void testEnableAndDisableConnection() throws Exception {
    loadConnections();

    // disable connection 1
    handler.enableConnection(1, false, getDerbyConnection());

    MConnection retrieved = handler.findConnection(1, getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable connection 1
    handler.enableConnection(1, true, getDerbyConnection());

    retrieved = handler.findConnection(1, getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  public void testDeleteConnection() throws Exception {
    loadConnections();

    handler.deleteConnection(1, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_CONNECTION", 1);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 4);

    handler.deleteConnection(2, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_CONNECTION", 0);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 0);
  }

  public MConnection getConnection() {
    return new MConnection(1,
      handler.findConnector("A", getDerbyConnection()).getConnectionForms(),
      handler.findFramework(getDerbyConnection()).getConnectionForms()
    );
  }
}

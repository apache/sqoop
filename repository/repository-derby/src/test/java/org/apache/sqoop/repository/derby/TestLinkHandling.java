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
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test link methods on Derby repository.
 */
public class TestLinkHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();

    // We always needs connector and framework structures in place
    loadConnectorAndDriverConfig();
  }

  public void testFindLink() throws Exception {
    // Let's try to find non existing link
    try {
      handler.findLink(1, getDerbyDatabaseConnection());
      fail();
    } catch(SqoopException ex) {
      assertEquals(DerbyRepoError.DERBYREPO_0024, ex.getErrorCode());
    }

    // Load prepared connections into database
    loadLinks();

    MLink connA = handler.findLink(1, getDerbyDatabaseConnection());
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

  public void testFindLinks() throws Exception {
    List<MLink> list;

    // Load empty list on empty repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(0, list.size());

    loadLinks();

    // Load all two connections on loaded repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(2, list.size());

    assertEquals("CA", list.get(0).getName());
    assertEquals("CB", list.get(1).getName());
  }

  public void testExistsLink() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsLink(1, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(2, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(3, getDerbyDatabaseConnection()));

    loadLinks();

    assertTrue(handler.existsLink(1, getDerbyDatabaseConnection()));
    assertTrue(handler.existsLink(2, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(3, getDerbyDatabaseConnection()));
  }

  public void testCreateLink() throws Exception {
    MLink link = getLink();

    // Load some data
    fillLink(link);

    handler.createLink(link, getDerbyDatabaseConnection());

    assertEquals(1, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 1);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 4);

    MLink retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertEquals(1, retrieved.getPersistenceId());

    List<MForm> forms;
    forms = link.getConnectorPart().getForms();
    assertEquals("Value1", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    forms = link.getFrameworkPart().getForms();
    assertEquals("Value13", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    // Let's create second link
    link = getLink();
    fillLink(link);

    handler.createLink(link, getDerbyDatabaseConnection());

    assertEquals(2, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 2);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 8);
  }

  public void testInUseLink() throws Exception {
    loadLinks();

    assertFalse(handler.inUseLink(1, getDerbyDatabaseConnection()));

    loadJobs();

    assertTrue(handler.inUseLink(1, getDerbyDatabaseConnection()));
  }

  public void testUpdateLink() throws Exception {
    loadLinks();

    MLink link = handler.findLink(1, getDerbyDatabaseConnection());

    List<MForm> forms;

    forms = link.getConnectorPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(null);
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(1).getInputs().get(1)).setValue(null);

    forms = link.getFrameworkPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    link.setName("name");

    handler.updateLink(link, getDerbyDatabaseConnection());

    assertEquals(1, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_CONNECTION", 2);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 10);

    MLink retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertEquals("name", link.getName());

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

  public void testEnableAndDisableLink() throws Exception {
    loadLinks();

    // disable link 1
    handler.enableLink(1, false, getDerbyDatabaseConnection());

    MLink retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable link 1
    handler.enableLink(1, true, getDerbyDatabaseConnection());

    retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  public void testDeleteLink() throws Exception {
    loadLinks();

    handler.deleteLink(1, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_CONNECTION", 1);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 4);

    handler.deleteLink(2, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_CONNECTION", 0);
    assertCountForTable("SQOOP.SQ_CONNECTION_INPUT", 0);
  }

  public MLink getLink() {
    return new MLink(1,
      handler.findConnector("A", getDerbyDatabaseConnection()).getConnectionForms(),
      handler.findDriverConfig(getDerbyDatabaseConnection()).getConnectionForms()
    );
  }
}

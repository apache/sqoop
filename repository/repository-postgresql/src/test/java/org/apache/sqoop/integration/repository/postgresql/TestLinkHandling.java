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

import java.util.List;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.SubmissionStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test link methods on Derby repository.
 */
@Test(groups = "postgresql")
public class TestLinkHandling extends PostgresqlTestCase {

  public static final String CONNECTOR_A_NAME = "A";
  public static final String CONNECTOR_A_CLASSNAME = "org.apache.sqoop.test.A";
  public static final String CONNECTOR_A_VERSION = "1.0-test";
  public static final String CONNECTOR_B_NAME = "B";
  public static final String CONNECTOR_B_CLASSNAME = "org.apache.sqoop.test.B";
  public static final String CONNECTOR_B_VERSION = "1.0-test";
  public static final String LINK_A_NAME = "Link-A";
  public static final String LINK_B_NAME = "Link-B";

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();

    handler.registerDriver(getDriver(), provider.getConnection());
    MConnector connectorA = getConnector(CONNECTOR_A_NAME, CONNECTOR_A_CLASSNAME, CONNECTOR_A_VERSION, true, true);
    MConnector connectorB = getConnector(CONNECTOR_B_NAME, CONNECTOR_B_CLASSNAME, CONNECTOR_B_VERSION, true, true);
    handler.registerConnector(connectorA, provider.getConnection());
    handler.registerConnector(connectorB, provider.getConnection());
    MLink linkA = getLink(LINK_A_NAME, connectorA);
    MLink linkB = getLink(LINK_B_NAME, connectorB);
    handler.createLink(linkA, provider.getConnection());
    handler.createLink(linkB, provider.getConnection());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testFindLinkFail() {
    // Delete links
    for (MLink link : handler.findLinks(provider.getConnection())) {
      handler.deleteLink(link.getPersistenceId(), provider.getConnection());
    }

    handler.findLink(1, provider.getConnection());
  }

  @Test
  public void testFindLinkSuccess() throws Exception {
    MLink linkA = handler.findLink(1, provider.getConnection());
    assertNotNull(linkA);
    assertEquals(1, linkA.getPersistenceId());
    assertEquals(LINK_A_NAME, linkA.getName());

    // Check connector link config
    List<MConfig> configs = linkA.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindLinkByName() throws Exception {
    // Load non-existing
    assertNull(handler.findLink("non-existing", provider.getConnection()));

    MLink linkA = handler.findLink(LINK_A_NAME, provider.getConnection());
    assertNotNull(linkA);
    assertEquals(1, linkA.getPersistenceId());
    assertEquals(LINK_A_NAME, linkA.getName());

    // Check connector link config
    List<MConfig> configs = linkA.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindLinks() throws Exception {
    List<MLink> list;

    // Load all two links on loaded repository
    list = handler.findLinks(provider.getConnection());
    assertEquals(2, list.size());
    assertEquals(LINK_A_NAME, list.get(0).getName());
    assertEquals(LINK_B_NAME, list.get(1).getName());

    // Delete links
    for (MLink link : handler.findLinks(provider.getConnection())) {
      handler.deleteLink(link.getPersistenceId(), provider.getConnection());
    }

    // Load empty list on empty repository
    list = handler.findLinks(provider.getConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testFindLinksByConnector() throws Exception {
    List<MLink> list;
    Long connectorId = handler.findConnector("A", provider.getConnection()).getPersistenceId();

    // Load all two links on loaded repository
    list = handler.findLinksForConnector(connectorId, provider.getConnection());
    assertEquals(1, list.size());
    assertEquals(LINK_A_NAME, list.get(0).getName());

    // Delete links
    for (MLink link : handler.findLinks(provider.getConnection())) {
      handler.deleteLink(link.getPersistenceId(), provider.getConnection());
    }

    // Load empty list on empty repository
    list = handler.findLinksForConnector(connectorId, provider.getConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testFindLinksByNonExistingConnector() throws Exception {
    List<MLink> list = handler.findLinksForConnector(11, provider.getConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testExistsLink() throws Exception {
    assertTrue(handler.existsLink(1, provider.getConnection()));
    assertTrue(handler.existsLink(2, provider.getConnection()));
    assertFalse(handler.existsLink(3, provider.getConnection()));

    // Delete links
    for (MLink link : handler.findLinks(provider.getConnection())) {
      handler.deleteLink(link.getPersistenceId(), provider.getConnection());
    }

    assertFalse(handler.existsLink(1, provider.getConnection()));
    assertFalse(handler.existsLink(2, provider.getConnection()));
    assertFalse(handler.existsLink(3, provider.getConnection()));
  }

  @Test
  public void testCreateLink() throws Exception {
    List<MConfig> configs;

    MLink retrieved = handler.findLink(1, provider.getConnection());
    assertEquals(1, retrieved.getPersistenceId());

    configs = retrieved.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    retrieved = handler.findLink(2, provider.getConnection());
    assertEquals(2, retrieved.getPersistenceId());

    configs = retrieved.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK")), 2);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK_INPUT")), 4);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testCreateDuplicateLink() throws Exception {
    MLink link = handler.findLink(LINK_A_NAME, provider.getConnection());
    link.setPersistenceId(MLink.PERSISTANCE_ID_DEFAULT);
    handler.createLink(link, provider.getConnection());
  }

  @Test
  public void testInUseLink() throws Exception {
    assertFalse(handler.inUseLink(1, provider.getConnection()));

    // Create job and submission and make that job in use to make sure link is in use.
    MLink linkA = handler.findLink(LINK_A_NAME, provider.getConnection());
    MJob job = getJob("Job-A",
        handler.findConnector("A", provider.getConnection()),
        handler.findConnector("B", provider.getConnection()),
        linkA,
        handler.findLink(LINK_B_NAME, provider.getConnection()));
    handler.createJob(job, provider.getConnection());
    MSubmission submission = getSubmission(job, SubmissionStatus.RUNNING);
    handler.createSubmission(submission, provider.getConnection());

    assertTrue(handler.inUseLink(linkA.getPersistenceId(), provider.getConnection()));
  }

  @Test
  public void testUpdateLink() throws Exception {
    MLink link = handler.findLink(1, provider.getConnection());

    List<MConfig> configs;

    configs = link.getConnectorLinkConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput) configs.get(0).getInputs().get(1)).setValue(null);
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput) configs.get(1).getInputs().get(1)).setValue(null);

    link.setName("name");

    handler.updateLink(link, provider.getConnection());

    assertEquals(1, link.getPersistenceId());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK")), 2);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK_INPUT")), 4);

    MLink retrieved = handler.findLink(1, provider.getConnection());
    assertEquals("name", link.getName());

    configs = retrieved.getConnectorLinkConfig().getConfigs();
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Updated", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testEnableAndDisableLink() throws Exception {
    // disable link 1
    handler.enableLink(1, false, provider.getConnection());

    MLink retrieved = handler.findLink(1, provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable link 1
    handler.enableLink(1, true, provider.getConnection());

    retrieved = handler.findLink(1, provider.getConnection());
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  @Test
  public void testDeleteLink() throws Exception {
    handler.deleteLink(1, provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK")), 1);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK_INPUT")), 2);

    handler.deleteLink(2, provider.getConnection());
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK")), 0);
    Assert.assertEquals(provider.rowCount(new TableName("SQOOP", "SQ_LINK_INPUT")), 0);
  }
}

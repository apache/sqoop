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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigUpdateEntityType;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.error.code.CommonRepositoryError;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test link methods on Derby repository.
 */
public class TestLinkHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    createOrUpgradeSchemaForLatestVersion();
    // We always needs connector and framework structures in place
    loadConnectorAndDriverConfig();
  }

  @Test
  public void testFindLink() throws Exception {
    // Let's try to find non existing link
    try {
      handler.findLink(1, getDerbyDatabaseConnection());
      fail();
    } catch(SqoopException ex) {
      assertEquals(CommonRepositoryError.COMMON_0021, ex.getErrorCode());
    }

    // Load prepared links into database
    loadLinksForLatestVersion();

    MLink linkA = handler.findLink(1, getDerbyDatabaseConnection());
    assertNotNull(linkA);
    assertEquals(1, linkA.getPersistenceId());
    assertEquals("CA", linkA.getName());

    List<MConfig> configs;

    // Check connector link config
    configs = linkA.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value3", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindLinkByName() throws Exception {
    // Let's try to find non existing link
    assertNull(handler.findLink("non-existing", getDerbyDatabaseConnection()));
    // Load prepared links into database
    loadLinksForLatestVersion();

    MLink linkA = handler.findLink("CA", getDerbyDatabaseConnection());
    assertNotNull(linkA);
    assertEquals(1, linkA.getPersistenceId());
    assertEquals("CA", linkA.getName());

    List<MConfig> configs;

    // Check connector link config
    configs = linkA.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value3", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testFindLinks() throws Exception {
    List<MLink> list;

    // Load empty list on empty repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(0, list.size());

    loadLinksForLatestVersion();

    // Load all two links on loaded repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(2, list.size());

    assertEquals("CA", list.get(0).getName());
    assertEquals("CB", list.get(1).getName());
  }

  @Test
  public void testFindLinksByConnector() throws Exception {
    List<MLink> list;

    // Load empty list on empty repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(0, list.size());

    loadLinksForLatestVersion();

    // Load all two links on loaded repository
    list = handler.findLinksForConnector(1, getDerbyDatabaseConnection());
    assertEquals(2, list.size());

    assertEquals("CA", list.get(0).getName());
    assertEquals("CB", list.get(1).getName());
  }

  public void testFindLinksByNonExistingConnector() throws Exception {
    List<MLink> list;

    // Load empty list on empty repository
    list = handler.findLinks(getDerbyDatabaseConnection());
    assertEquals(0, list.size());

    loadLinksForLatestVersion();

    list = handler.findLinksForConnector(2, getDerbyDatabaseConnection());
    assertEquals(0, list.size());
  }

  @Test
  public void testExistsLink() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsLink(1, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(2, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(3, getDerbyDatabaseConnection()));

    loadLinksForLatestVersion();

    assertTrue(handler.existsLink(1, getDerbyDatabaseConnection()));
    assertTrue(handler.existsLink(2, getDerbyDatabaseConnection()));
    assertFalse(handler.existsLink(3, getDerbyDatabaseConnection()));
  }

  @Test
  public void testCreateLink() throws Exception {
    MLink link = getLink();

    // Load some data
    fillLink(link);

    handler.createLink(link, getDerbyDatabaseConnection());

    assertEquals(1, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_LINK", 1);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 2);

    MLink retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertEquals(1, retrieved.getPersistenceId());

    List<MConfig> configs;
    configs = link.getConnectorLinkConfig().getConfigs();
    assertEquals("Value1", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Value2", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());

    // Let's create second link
    link = getLink();
    fillLink(link);

    handler.createLink(link, getDerbyDatabaseConnection());

    assertEquals(2, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_LINK", 2);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 4);
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testCreateDuplicateLink() throws Exception {
    MLink link = getLink();
    fillLink(link);
    link.setName("test");
    handler.createLink(link, getDerbyDatabaseConnection());
    assertEquals(1, link.getPersistenceId());

    link.setPersistenceId(MLink.PERSISTANCE_ID_DEFAULT);
    handler.createLink(link, getDerbyDatabaseConnection());
  }

  @Test
  public void testInUseLink() throws Exception {
    loadLinksForLatestVersion();

    assertFalse(handler.inUseLink(1, getDerbyDatabaseConnection()));

    loadJobsForLatestVersion();

    assertTrue(handler.inUseLink(1, getDerbyDatabaseConnection()));
  }

  @Test
  public void testUpdateLink() throws Exception {
    loadLinksForLatestVersion();

    MLink link = handler.findLink(1, getDerbyDatabaseConnection());

    List<MConfig> configs;

    configs = link.getConnectorLinkConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput) configs.get(0).getInputs().get(1)).setValue(null);
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput) configs.get(1).getInputs().get(1)).setValue(null);

    link.setName("name");

    handler.updateLink(link, getDerbyDatabaseConnection());

    assertEquals(1, link.getPersistenceId());
    assertCountForTable("SQOOP.SQ_LINK", 2);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 6);

    MLink retrieved = handler.findLink(1, getDerbyDatabaseConnection());
    assertEquals("name", link.getName());

    configs = retrieved.getConnectorLinkConfig().getConfigs();
    assertEquals("Updated", configs.get(0).getInputs().get(0).getValue());
    assertNull(configs.get(0).getInputs().get(1).getValue());
    assertEquals("Updated", configs.get(1).getInputs().get(0).getValue());
    assertNull(configs.get(1).getInputs().get(1).getValue());
  }

  @Test
  public void testEnableAndDisableLink() throws Exception {
    loadLinksForLatestVersion();

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

  @Test
  public void testDeleteLink() throws Exception {
    loadLinksForLatestVersion();

    handler.deleteLink(1, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_LINK", 1);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 4);

    handler.deleteLink(2, getDerbyDatabaseConnection());
    assertCountForTable("SQOOP.SQ_LINK", 0);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 0);
  }

  @Test
  public void testUpdateLinkConfig() throws Exception {
    loadLinksForLatestVersion();

    assertCountForTable("SQOOP.SQ_LINK", 2);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 8);
    MLink link = handler.findLink(1, getDerbyDatabaseConnection());

    List<MConfig> configs = link.getConnectorLinkConfig().getConfigs();
    MConfig config = configs.get(0).clone(false);
    MConfig newConfig = new MConfig(config.getName(), config.getInputs());

    ((MStringInput) newConfig.getInputs().get(0)).setValue("LinkConfigUpdated");

    handler.updateLinkConfig(link.getPersistenceId(), newConfig, MConfigUpdateEntityType.USER,
        getDerbyDatabaseConnection());

    MLink updatedLink = handler.findLink(1, getDerbyDatabaseConnection());
    MLinkConfig newConfigs = updatedLink.getConnectorLinkConfig();
    assertEquals(2, newConfigs.getConfigs().size());
    MConfig updatedLinkConfig = newConfigs.getConfigs().get(0);
    assertEquals("LinkConfigUpdated", updatedLinkConfig.getInputs().get(0).getValue());
  }

  @Test(expectedExceptions = SqoopException.class)
  public void testNonExistingLinkConfigFetch() throws Exception {
    loadLinksForLatestVersion();
    assertCountForTable("SQOOP.SQ_LINK", 2);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 8);
    handler.findLinkConfig(1, "Non-ExistingC1LINK1", getDerbyDatabaseConnection());
  }

  @Test
  public void testLinkConfigFetch() throws Exception {
    loadLinksForLatestVersion();
    assertCountForTable("SQOOP.SQ_LINK", 2);
    assertCountForTable("SQOOP.SQ_LINK_INPUT", 8);
    MConfig config = handler.findLinkConfig(1, "C1LINK0", getDerbyDatabaseConnection());
    assertEquals("Value1", config.getInputs().get(0).getValue());
    assertNull(config.getInputs().get(1).getValue());
  }

  public MLink getLink() {
    return new MLink(1, handler.findConnector("A", getDerbyDatabaseConnection()).getLinkConfig());
  }
}

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
import static org.junit.Assert.assertNotSame;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MStringInput;
import org.junit.Before;
import org.junit.Test;

/**
 * Test proper support of all available model types.
 */
public class TestInputTypes extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();
    // We always needs schema for this test case
    createOrUpgradeSchemaForLatestVersion();
  }

  /**
   * Ensure that metadata with all various data types can be successfully
   * serialized into repository and retrieved back.
   */
  @Test
  public void testEntitySerialization() throws Exception {
    MConnector connector = getConnector();

    // Serialize the connector with all data types into repository
    handler.registerConnector(connector, getDerbyDatabaseConnection());

    // Successful serialization should update the ID
    assertNotSame(connector.getPersistenceId(), MPersistableEntity.PERSISTANCE_ID_DEFAULT);

    // Retrieve registered connector
    MConnector retrieved = handler.findConnector(connector.getUniqueName(),
        getDerbyDatabaseConnection());
    assertNotNull(retrieved);

    // Original and retrieved connectors should be the same
    assertEquals(connector, retrieved);
  }

  /**
   * Test that serializing actual data is not an issue.
   */
  @Test
  public void testEntityDataSerialization() throws Exception {
    MConnector connector = getConnector();
    MDriver driver = getDriver();

    // Register objects for everything and our new connector
    handler.registerConnector(connector, getDerbyDatabaseConnection());
    handler.registerDriver(driver, getDerbyDatabaseConnection());

    // Inserted values
    Map<String, String> map = new HashMap<String, String>();
    map.put("A", "B");

    // Connection object with all various values
    MLink link = new MLink(connector.getPersistenceId(), connector.getLinkConfig());
    MLinkConfig linkConfig = link.getConnectorLinkConfig();
    linkConfig.getStringInput("l1.String").setValue("A");
    linkConfig.getMapInput("l1.Map").setValue(map);
    linkConfig.getIntegerInput("l1.Integer").setValue(1);
    linkConfig.getBooleanInput("l1.Boolean").setValue(true);
    linkConfig.getEnumInput("l1.Enum").setValue("YES");

    // Create the link in repository
    handler.createLink(link, getDerbyDatabaseConnection());
    assertNotSame(link.getPersistenceId(), MPersistableEntity.PERSISTANCE_ID_DEFAULT);

    // Retrieve created link
    MLink retrieved = handler.findLink(link.getPersistenceId(), getDerbyDatabaseConnection());
    linkConfig = retrieved.getConnectorLinkConfig();
    assertEquals("A", linkConfig.getStringInput("l1.String").getValue());
    assertEquals(map, linkConfig.getMapInput("l1.Map").getValue());
    assertEquals(1, (int) linkConfig.getIntegerInput("l1.Integer").getValue());
    assertEquals(true, (boolean) linkConfig.getBooleanInput("l1.Boolean").getValue());
    assertEquals("YES", linkConfig.getEnumInput("l1.Enum").getValue());
  }

  /**
   * Overriding parent method to get forms with all supported data types.
   *
   * @return Forms with all data types
   */
  @Override
  protected List<MConfig> getConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs;
    MInput input;

    inputs = new LinkedList<MInput<?>>();

    input = new MStringInput(configName1 + ".String", false, (short) 30);
    inputs.add(input);

    input = new MMapInput(configName1 + ".Map", false);
    inputs.add(input);

    input = new MIntegerInput(configName1 + ".Integer", false);
    inputs.add(input);

    input = new MBooleanInput(configName1 + ".Boolean", false);
    inputs.add(input);

    input = new MEnumInput(configName1 + ".Enum", false, new String[] { "YES", "NO" });
    inputs.add(input);

    configs.add(new MConfig(configName1, inputs));
    return configs;
  }
}

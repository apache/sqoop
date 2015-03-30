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
package org.apache.sqoop.model;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.testng.annotations.Test;

public class TestMConnector {

  private MConnector createConnector(List<Direction> supportedDirections) {
    List<MConfig> configs = new ArrayList<MConfig>();
    MIntegerInput inputs = new MIntegerInput("INTEGER-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.setValue(100);
    MStringInput strInput = new MStringInput("STRING-INPUT",false, InputEditable.ANY, StringUtils.EMPTY, (short)20);
    strInput.setValue("TEST-VALUE");
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(inputs);
    list.add(strInput);
    MConfig config = new MConfig("CONFIGNAME", list);
    configs.add(config);

    MLinkConfig linkConfig = new MLinkConfig(configs);
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;

    if (supportedDirections.contains(Direction.FROM)) {
      fromConfig = new MFromConfig(configs);
    }

    if (supportedDirections.contains(Direction.TO)) {
      toConfig = new MToConfig(configs);
    }

    return new MConnector("NAME", "CLASSNAME", "1.0",
        linkConfig, fromConfig, toConfig);
  }

  /**
   * Test for initialization
   */
  @Test
  public void testInitialization() {
    List<MConfig> fromJobConfig = new ArrayList<MConfig>();
    List<MConfig> toJobConfig = new ArrayList<MConfig>();
    MLinkConfig linkConfig = new MLinkConfig(fromJobConfig);
    MFromConfig fromConfig1 = new MFromConfig(fromJobConfig);
    MToConfig toConfig1 = new MToConfig(toJobConfig);
    MConnector connector1 = new MConnector("NAME", "CLASSNAME", "1.0",
        linkConfig, fromConfig1, toConfig1);
    assertEquals("NAME", connector1.getUniqueName());
    assertEquals("CLASSNAME", connector1.getClassName());
    assertEquals("1.0", connector1.getVersion());
    MConnector connector2 = new MConnector("NAME", "CLASSNAME", "1.0",
        linkConfig, fromConfig1, toConfig1);
    assertEquals(connector2, connector1);
    MConnector connector3 = new MConnector("NAME1", "CLASSNAME", "2.0",
        linkConfig, fromConfig1, toConfig1);
    assertFalse(connector1.equals(connector3));

    try {
      connector1 = new MConnector(null, "CLASSNAME", "1.0", linkConfig,
          fromConfig1, toConfig1); // Expecting null pointer exception
    } catch (NullPointerException e) {
      assertTrue(true);
    }
    try {
      connector1 = new MConnector("NAME", null, "1.0", linkConfig,
          fromConfig1, toConfig1); // Expecting null pointer exception
    } catch (NullPointerException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testClone() {
    MConnector connector = createConnector(Arrays.asList(Direction.FROM, Direction.TO));
    assertEquals("NAME", connector.getUniqueName());
    assertEquals("CLASSNAME", connector.getClassName());
    assertEquals("1.0", connector.getVersion());
    //Clone with values. Checking values copying after the cloning. But config values will be null
    MConnector cloneConnector1 = connector.clone(true);
    assertEquals("NAME", cloneConnector1.getUniqueName());
    assertEquals("CLASSNAME", cloneConnector1.getClassName());
    assertEquals("1.0", cloneConnector1.getVersion());
    MConfig clonedLinkConfig = cloneConnector1.getLinkConfig().getConfigs().get(0);
    assertNull(clonedLinkConfig.getInputs().get(0).getValue());
    assertNull(clonedLinkConfig.getInputs().get(1).getValue());

    MConfig clonedFromConfig = cloneConnector1.getFromConfig().getConfigs().get(0);
    assertNull(clonedFromConfig.getInputs().get(0).getValue());
    assertNull(clonedFromConfig.getInputs().get(1).getValue());

    MConfig clonedToConfig = cloneConnector1.getToConfig().getConfigs().get(0);
    assertNull(clonedToConfig.getInputs().get(0).getValue());
    assertNull(clonedToConfig.getInputs().get(1).getValue());

    //Clone without values. Inputs value will be null after cloning.
    MConnector clonedConnector2 = connector.clone(false);
    clonedLinkConfig = clonedConnector2.getLinkConfig().getConfigs().get(0);
    assertNull(clonedLinkConfig.getInputs().get(0).getValue());
    assertNull(clonedLinkConfig.getInputs().get(1).getValue());
    clonedFromConfig = clonedConnector2.getFromConfig().getConfigs().get(0);
    assertNull(clonedFromConfig.getInputs().get(0).getValue());
    assertNull(clonedFromConfig.getInputs().get(1).getValue());
    clonedToConfig = clonedConnector2.getToConfig().getConfigs().get(0);
    assertNull(clonedToConfig.getInputs().get(0).getValue());
    assertNull(clonedToConfig.getInputs().get(1).getValue());
  }

  @Test
  public void testFromDirection() {
    MConnector connector = createConnector(Arrays.asList(Direction.FROM));

    // Clone should clone only one job config.
    MConnector clone = connector.clone(true);
    assertNotNull(clone.getFromConfig());
    assertNull(clone.getToConfig());
    assertEquals(connector, clone);
    assertEquals(connector.toString(), clone.toString());
    assertNotEquals(connector.hashCode(), clone.hashCode());
  }

  @Test
  public void testToDirection() {
    MConnector connector = createConnector(Arrays.asList(Direction.TO));

    // Clone should clone only one job config.
    MConnector clone = connector.clone(true);
    assertNull(clone.getFromConfig());
    assertNotNull(clone.getToConfig());
    assertEquals(connector, clone);
    assertEquals(connector.toString(), clone.toString());
    assertNotEquals(connector.hashCode(), clone.hashCode());
  }

  @Test
  public void testNoDirection() {
    MConnector connector = createConnector(Arrays.asList(new Direction[0]));

    // Clone should clone only one job config.
    MConnector clone = connector.clone(true);
    assertNull(clone.getFromConfig());
    assertNull(clone.getToConfig());
    assertEquals(connector, clone);
    assertEquals(connector.toString(), clone.toString());
    assertNotEquals(connector.hashCode(), clone.hashCode());
  }

  @Test
  public void testBothDirections() {
    MConnector connector = createConnector(Arrays.asList(Direction.FROM, Direction.TO));

    // Clone should clone only one job config.
    MConnector clone = connector.clone(true);
    assertNotNull(clone.getFromConfig());
    assertNotNull(clone.getToConfig());
    assertEquals(connector, clone);
    assertEquals(connector.toString(), clone.toString());
    assertNotEquals(connector.hashCode(), clone.hashCode());
  }

  @Test
  public void testGetSupportedDirections() {
    MConnector connector = createConnector(Arrays.asList(Direction.FROM, Direction.TO));
    assertTrue(connector.getSupportedDirections().isDirectionSupported(Direction.FROM));
    assertTrue(connector.getSupportedDirections().isDirectionSupported(Direction.TO));

    connector = createConnector(Arrays.asList(Direction.FROM));
    assertTrue(connector.getSupportedDirections().isDirectionSupported(Direction.FROM));
    assertFalse(connector.getSupportedDirections().isDirectionSupported(Direction.TO));

    connector = createConnector(Arrays.asList(Direction.TO));
    assertFalse(connector.getSupportedDirections().isDirectionSupported(Direction.FROM));
    assertTrue(connector.getSupportedDirections().isDirectionSupported(Direction.TO));

    connector = createConnector(Arrays.asList(new Direction[]{}));
    assertFalse(connector.getSupportedDirections().isDirectionSupported(Direction.FROM));
    assertFalse(connector.getSupportedDirections().isDirectionSupported(Direction.TO));
  }
}

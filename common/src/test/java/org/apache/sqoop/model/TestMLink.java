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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestMLink {

  /**
   * Test for initialization
   */
  @Test
  public void testInitialization() {
    // Test default constructor
    MLink link = link();
    assertEquals(123l, link.getConnectorId());
    assertEquals("Vampire", link.getName());
    assertEquals("Buffy", link.getCreationUser());
    assertEquals(linkConfig(), link.getConnectorLinkConfig());

    // Test copy constructor
    MLink copy = new MLink(link);
    assertEquals(123l, copy.getConnectorId());
    assertEquals("Vampire", copy.getName());
    assertEquals("Buffy", copy.getCreationUser());
    assertEquals(link.getCreationDate(), copy.getCreationDate());
    assertEquals(linkConfig(), copy.getConnectorLinkConfig());
  }

  @Test
  public void testClone() {
    MLink link = link();

    // Clone without value
    MLink withoutLinkValue = link.clone(false);
    assertEquals(link, withoutLinkValue);
    assertEquals(MPersistableEntity.PERSISTANCE_ID_DEFAULT, withoutLinkValue.getPersistenceId());
    assertNull(withoutLinkValue.getName());
    assertNull(withoutLinkValue.getCreationUser());
    assertEquals(linkConfig(), withoutLinkValue.getConnectorLinkConfig());
    assertNull(withoutLinkValue.getConnectorLinkConfig().getConfig("CONFIGNAME").getInput("INTEGER-INPUT").getValue());
    assertNull(withoutLinkValue.getConnectorLinkConfig().getConfig("CONFIGNAME").getInput("STRING-INPUT").getValue());

    // Clone with value
    MLink withLinkValue = link.clone(true);
    assertEquals(link, withLinkValue);
    assertEquals(link.getPersistenceId(), withLinkValue.getPersistenceId());
    assertEquals(link.getName(), withLinkValue.getName());
    assertEquals(link.getCreationUser(), withLinkValue.getCreationUser());
    assertEquals(linkConfig(), withLinkValue.getConnectorLinkConfig());
    assertEquals(100, withLinkValue.getConnectorLinkConfig().getConfig("CONFIGNAME").getInput("INTEGER-INPUT").getValue());
    assertEquals("TEST-VALUE", withLinkValue.getConnectorLinkConfig().getConfig("CONFIGNAME").getInput("STRING-INPUT").getValue());
  }

  private MLink link() {
    MLink link = new MLink(123l, linkConfig());
    link.setName("Vampire");
    link.setCreationUser("Buffy");
    return link;
  }

  private MLinkConfig linkConfig() {
    List<MConfig> configs = new ArrayList<MConfig>();
    MIntegerInput input = new MIntegerInput("INTEGER-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    input.setValue(100);
    MStringInput strInput = new MStringInput("STRING-INPUT",false, InputEditable.ANY, StringUtils.EMPTY, (short)20);
    strInput.setValue("TEST-VALUE");
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    list.add(strInput);
    MConfig config = new MConfig("CONFIGNAME", list);
    configs.add(config);
    return new MLinkConfig(configs);
  }

}

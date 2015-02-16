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
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test class for org.apache.sqoop.model.MAccountableEntity
 */
public class TestMAccountableEntity {

  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    List<MConfig> configs = new ArrayList<MConfig>();
    MIntegerInput intInput = new MIntegerInput("INTEGER-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    MLongInput longInput = new MLongInput("LONG-INPUT", false, InputEditable.ANY, StringUtils.EMPTY);
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(intInput);
    list.add(longInput);

    MConfig config = new MConfig("CONFIGNAME", list);
    configs.add(config);
    MAccountableEntity link = new MLink(123l, new MLinkConfig(configs));
    // Initially creation date and last update date is same
    assertEquals(link.getCreationDate(), link.getLastUpdateDate());
    Date testCreationDate = new Date();
    Date testLastUpdateDate = new Date();
    link.setCreationUser("admin");
    link.setCreationDate(testCreationDate);
    link.setLastUpdateUser("user");
    link.setLastUpdateDate(testLastUpdateDate);
    link.setEnabled(false);
    assertEquals(testCreationDate, link.getCreationDate());
    assertEquals("admin", link.getCreationUser());
    assertEquals(testLastUpdateDate, link.getLastUpdateDate());
    assertEquals(false, link.getEnabled());
    assertEquals("user", link.getLastUpdateUser());
    assertEquals(1, ((MLink) link).getConnectorLinkConfig().getConfigs().size());
    assertEquals(2, ((MLink) link).getConnectorLinkConfig().getConfigs().get(0).getInputs().size());

  }
}

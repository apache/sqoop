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
package org.apache.sqoop.json;

import static org.apache.sqoop.json.ConfigTestUtil.getLink;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.apache.sqoop.json.util.ConfigInputConstants;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

/**
 *
 */
public class TestLinkBean {
  @Test
  public void testSerialization() {
    Date created = new Date();
    Date updated = new Date();
    MLink link = getLink("ahoj");
    link.setName("Connection");
    link.setPersistenceId(666);
    link.setCreationUser("admin");
    link.setCreationDate(created);
    link.setLastUpdateUser("user");
    link.setLastUpdateDate(updated);
    link.setEnabled(false);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) link.getConnectorLinkConfig().getConfigs()
      .get(0).getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinkBean linkBean = new LinkBean(link);
    JSONObject json = linkBean.extract(false);

    // Check for sensitivity
    JSONArray all = (JSONArray)json.get(JsonBean.ALL);
    JSONObject allItem = (JSONObject)all.get(0);
    JSONArray connectors = (JSONArray)allItem.get(LinkBean.LINK_CONFIG);
    JSONObject connector = (JSONObject)connectors.get(0);
    JSONArray inputs = (JSONArray)connector.get(ConfigInputConstants.CONFIG_INPUTS);
    for (Object input1 : inputs) {
      assertTrue(((JSONObject)input1).containsKey(ConfigInputConstants.CONFIG_INPUT_SENSITIVE));
    }

    // "Move" it across network in text form
    String linkJsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedLinkJson = (JSONObject) JSONValue.parse(linkJsonString);
    LinkBean retrievedBean = new LinkBean();
    retrievedBean.restore(parsedLinkJson);
    MLink target = retrievedBean.getLinks().get(0);

    // Check id and name
    assertEquals(666, target.getPersistenceId());
    assertEquals("Connection", target.getName());
    assertEquals("admin", target.getCreationUser());
    assertEquals(created, target.getCreationDate());
    assertEquals("user", target.getLastUpdateUser());
    assertEquals(updated, target.getLastUpdateDate());
    assertEquals(false, target.getEnabled());

    // Test that value was correctly moved
    MStringInput targetInput = (MStringInput) target.getConnectorLinkConfig()
      .getConfigs().get(0).getInputs().get(0);
    assertEquals("Hi there!", targetInput.getValue());
  }

  @Test
  public void testSensitivityFilter() {
    Date created = new Date();
    Date updated = new Date();
    MLink link = getLink("ahoj");
    link.setName("Connection");
    link.setPersistenceId(666);
    link.setCreationUser("admin");
    link.setCreationDate(created);
    link.setLastUpdateUser("user");
    link.setLastUpdateDate(updated);
    link.setEnabled(true);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) link.getConnectorLinkConfig().getConfigs()
      .get(0).getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinkBean bean = new LinkBean(link);
    JSONObject json = bean.extract(false);
    JSONObject jsonFiltered = bean.extract(true);

    // Sensitive values should exist
    JSONArray all = (JSONArray)json.get(JsonBean.ALL);
    JSONObject allItem = (JSONObject)all.get(0);
    JSONArray connectors = (JSONArray)allItem.get(LinkBean.LINK_CONFIG);
    JSONObject connector = (JSONObject)connectors.get(0);
    JSONArray inputs = (JSONArray)connector.get(ConfigInputConstants.CONFIG_INPUTS);
    assertEquals(3, inputs.size());
    // Inputs are ordered when creating link
    JSONObject password = (JSONObject)inputs.get(2);
    assertTrue(password.containsKey(ConfigInputConstants.CONFIG_INPUT_VALUE));

    // Sensitive values should not exist
    all = (JSONArray)jsonFiltered.get(JsonBean.ALL);
    allItem = (JSONObject)all.get(0);
    connectors = (JSONArray)allItem.get(LinkBean.LINK_CONFIG);
    connector = (JSONObject)connectors.get(0);
    inputs = (JSONArray)connector.get(ConfigInputConstants.CONFIG_INPUTS);
    assertEquals(3, inputs.size());
    // Inputs are ordered when creating link
    password = (JSONObject)inputs.get(2);
    assertFalse(password.containsKey(ConfigInputConstants.CONFIG_INPUT_VALUE));
  }
}

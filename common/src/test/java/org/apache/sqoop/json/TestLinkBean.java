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

import static org.apache.sqoop.json.util.ConfigInputSerialization.restoreValidator;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.sqoop.json.util.BeanTestUtil;
import org.apache.sqoop.json.util.ConfigInputConstants;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MValidator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

public class TestLinkBean {

  @Test
  public void testLinkSerialization() {
    Date created = new Date();
    Date updated = new Date();
    MLink link = BeanTestUtil.createLink("ahoj", "link1", 22L, created, updated);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) link.getConnectorLinkConfig().getConfigs().get(0)
        .getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinkBean linkBean = new LinkBean(link);
    JSONObject json = linkBean.extract(false);

    // Check for sensitivity
    JSONObject linkObj = (JSONObject)((JSONArray) json.get(LinkBean.LINKS)).get(0);
    JSONObject linkConfigList = (JSONObject) linkObj.get(LinkBean.LINK_CONFIG_VALUES);
    JSONArray linkConfigs = (JSONArray) linkConfigList.get(ConfigInputConstants.CONFIGS);
    List<MValidator> linkValidators = restoreValidator((JSONArray)
      linkConfigList.get(ConfigInputConstants.CONFIG_VALIDATORS));
    MValidator mValidator = linkValidators.get(0);
    assertEquals("testValidator1", mValidator.getValidatorClass());
    assertEquals("", mValidator.getStrArg());
    JSONObject linkConfig = (JSONObject) linkConfigs.get(0);
    JSONArray inputs = (JSONArray) linkConfig.get(ConfigInputConstants.CONFIG_INPUTS);
    for (Object in : inputs) {
      assertTrue(((JSONObject) in).containsKey(ConfigInputConstants.CONFIG_INPUT_SENSITIVE));
    }

    // "Move" it across network in text form
    String linkJsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedLinkJson = JSONUtils.parse(linkJsonString);
    LinkBean retrievedBean = new LinkBean();
    retrievedBean.restore(parsedLinkJson);
    MLink retrievedLink = retrievedBean.getLinks().get(0);

    // Check id and name
    assertEquals(22L, retrievedLink.getPersistenceId());
    assertEquals("link1", retrievedLink.getName());
    assertEquals("admin", retrievedLink.getCreationUser());
    assertEquals(created, retrievedLink.getCreationDate());
    assertEquals("user", retrievedLink.getLastUpdateUser());
    assertEquals(updated, retrievedLink.getLastUpdateDate());
    assertEquals(false, retrievedLink.getEnabled());

    // Test that value was correctly moved
    MStringInput retrievedLinkInput = (MStringInput) retrievedLink.getConnectorLinkConfig().getConfigs().get(0)
        .getInputs().get(0);
    assertEquals("Hi there!", retrievedLinkInput.getValue());
  }

  @Test
  public void testSensitivityFilter() {
    Date created = new Date();
    Date updated = new Date();
    MLink link = BeanTestUtil.createLink("ahoj", "link1", 22L, created, updated);


    // Fill some data at the beginning
    MStringInput input = (MStringInput) link.getConnectorLinkConfig().getConfigs().get(0)
        .getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinkBean bean = new LinkBean(link);
    JSONObject json = bean.extract(false);
    JSONObject jsonFiltered = bean.extract(true);

    // Sensitive values should exist
    JSONObject linkObj = (JSONObject)((JSONArray) json.get(LinkBean.LINKS)).get(0);
    JSONObject linkConfigList = (JSONObject) linkObj.get(LinkBean.LINK_CONFIG_VALUES);
    JSONArray linkConfigs = (JSONArray) linkConfigList.get(ConfigInputConstants.CONFIGS);
    JSONObject linkConfigObj = (JSONObject) linkConfigs.get(0);
    JSONArray inputs = (JSONArray) linkConfigObj.get(ConfigInputConstants.CONFIG_INPUTS);
    assertEquals(3, inputs.size());
    // Inputs are ordered when creating link
    JSONObject password = (JSONObject) inputs.get(2);
    assertTrue(password.containsKey(ConfigInputConstants.CONFIG_INPUT_VALUE));

    // Sensitive values should not exist
    linkObj = (JSONObject)((JSONArray) jsonFiltered.get(LinkBean.LINKS)).get(0);
    linkConfigList = (JSONObject) linkObj.get(LinkBean.LINK_CONFIG_VALUES);
    linkConfigs = (JSONArray) linkConfigList.get(ConfigInputConstants.CONFIGS);
    linkConfigObj = (JSONObject) linkConfigs.get(0);
    inputs = (JSONArray) linkConfigObj.get(ConfigInputConstants.CONFIG_INPUTS);
    assertEquals(3, inputs.size());
    // Inputs are ordered when creating link
    password = (JSONObject) inputs.get(2);
    assertFalse(password.containsKey(ConfigInputConstants.CONFIG_INPUT_VALUE));
  }

  @Test
  public void testLinksSerialization() {
    Date created = new Date();
    Date updated = new Date();
    MLink link1 = BeanTestUtil.createLink("ahoj", "link1", 666L, created, updated);
    MLink link2 = BeanTestUtil.createLink("jhoa", "link2", 888L, created, updated);

    List<MLink> links = new ArrayList<>();
    links.add(link1);
    links.add(link2);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) link1.getConnectorLinkConfig().getConfigs().get(0)
        .getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinkBean linkBean = new LinkBean(links);
    JSONObject json = linkBean.extract(false);

    // Check for sensitivity
    JSONArray linksObj = (JSONArray) json.get(LinkBean.LINKS);
    JSONObject linkObj = (JSONObject) linksObj.get(0);

    JSONObject linkConfigList = (JSONObject) linkObj.get(LinkBean.LINK_CONFIG_VALUES);
    JSONArray linkConfigs = (JSONArray) linkConfigList.get(ConfigInputConstants.CONFIGS);
    JSONObject linkConfig = (JSONObject) linkConfigs.get(0);
    JSONArray inputs = (JSONArray) linkConfig.get(ConfigInputConstants.CONFIG_INPUTS);
    for (Object inp : inputs) {
      assertTrue(((JSONObject) inp).containsKey(ConfigInputConstants.CONFIG_INPUT_SENSITIVE));
    }

    // "Move" it across network in text form
    String linkJsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedLinkJson = JSONUtils.parse(linkJsonString);
    LinkBean retrievedBean = new LinkBean();
    retrievedBean.restore(parsedLinkJson);
    MLink targetLink1 = retrievedBean.getLinks().get(0);
    MLink targetLink2 = retrievedBean.getLinks().get(1);

    // Check id and name
    assertEquals(666L, targetLink1.getPersistenceId());
    assertEquals(888L, targetLink2.getPersistenceId());

    assertEquals("link1", targetLink1.getName());
    assertEquals("link2", targetLink2.getName());

    assertEquals("admin", targetLink1.getCreationUser());
    assertEquals(created, targetLink1.getCreationDate());
    assertEquals("user", targetLink1.getLastUpdateUser());
    assertEquals(updated, targetLink1.getLastUpdateDate());
    assertEquals(false, targetLink1.getEnabled());

    // Test that value was correctly moved
    MStringInput targetInput = (MStringInput) targetLink1.getConnectorLinkConfig().getConfigs()
        .get(0).getInputs().get(0);
    assertEquals("Hi there!", targetInput.getValue());
  }
}

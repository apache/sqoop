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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.sqoop.json.util.BeanTestUtil;
import org.apache.sqoop.json.util.ConfigInputConstants;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

public class TestLinksBean {

  @Test
  public void testLinksSerialization() {
    Date created = new Date();
    Date updated = new Date();
    MLink link1 = BeanTestUtil.createLink("ahoj", "link1", 666L, created, updated);
    MLink link2 = BeanTestUtil.createLink("jhoa", "link2", 888L, created, updated);

    List<MLink> links = new ArrayList<MLink>();
    links.add(link1);
    links.add(link2);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) link1.getConnectorLinkConfig().getConfigs().get(0)
        .getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    LinksBean linkBean = new LinksBean(links);
    JSONObject json = linkBean.extract(false);

    // Check for sensitivity
    JSONArray linksObj = (JSONArray) json.get(LinksBean.LINKS);
    JSONObject linkObj = (JSONObject) linksObj.get(0);

    JSONArray linkConfigs = (JSONArray) linkObj.get(LinkBean.LINK_CONFIG_VALUES);
    JSONObject linkConfig = (JSONObject) linkConfigs.get(0);
    JSONArray inputs = (JSONArray) linkConfig.get(ConfigInputConstants.CONFIG_INPUTS);
    for (Object inp : inputs) {
      assertTrue(((JSONObject) inp).containsKey(ConfigInputConstants.CONFIG_INPUT_SENSITIVE));
    }

    // "Move" it across network in text form
    String linkJsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedLinkJson = JSONUtils.parse(linkJsonString);
    LinksBean retrievedBean = new LinksBean();
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

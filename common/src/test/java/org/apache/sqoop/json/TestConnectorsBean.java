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
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.sqoop.json.util.BeanTestUtil;
import org.apache.sqoop.json.util.ConfigTestUtil;
import org.apache.sqoop.model.MConnector;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

public class TestConnectorsBean {

  @Test
  public void testConnectorsSerialization() {
    // Create testing connector
    List<MConnector> connectors = new LinkedList<MConnector>();
    connectors.add(BeanTestUtil.getConnector(1L, "jdbc"));
    connectors.add(BeanTestUtil.getConnector(2L, "mysql"));

    // Create testing bundles
    Map<Long, ResourceBundle> configBundles = new HashMap<Long, ResourceBundle>();
    configBundles.put(1L, ConfigTestUtil.getResourceBundle());
    configBundles.put(2L, ConfigTestUtil.getResourceBundle());

    // Serialize it to JSON object
    ConnectorsBean connectorsBean = new ConnectorsBean(connectors, configBundles);
    JSONObject connectorsJSON = connectorsBean.extract(false);

    // "Move" it across network in text form
    String connectorsJSONString = connectorsJSON.toJSONString();

    // Retrieved transferred object
    JSONObject parsedConnectors = JSONUtils.parse(connectorsJSONString);
    ConnectorsBean parsedConnectorsBean = new ConnectorsBean();
    parsedConnectorsBean.restore(parsedConnectors);

    assertEquals(connectors.size(), parsedConnectorsBean.getConnectors().size());
    assertEquals(connectors.get(0), parsedConnectorsBean.getConnectors().get(0));
    assertEquals(connectors.get(1), parsedConnectorsBean.getConnectors().get(1));

    ResourceBundle retrievedBundle = parsedConnectorsBean.getResourceBundles().get(1L);
    assertNotNull(retrievedBundle);
    assertEquals("a", retrievedBundle.getString("a"));
    assertEquals("b", retrievedBundle.getString("b"));
  }

  @Test
  public void testSingleDirection() {
    // Create testing connector
    List<MConnector> connectors = new LinkedList<MConnector>();
    connectors.add(BeanTestUtil.getConnector(1L, "jdbc", true, false));
    connectors.add(BeanTestUtil.getConnector(2L, "mysql", false, true));

    // Create testing bundles
    Map<Long, ResourceBundle> bundles = new HashMap<Long, ResourceBundle>();
    bundles.put(1L, ConfigTestUtil.getResourceBundle());
    bundles.put(2L, ConfigTestUtil.getResourceBundle());

    // Serialize it to JSON object
    ConnectorsBean bean = new ConnectorsBean(connectors, bundles);
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = JSONUtils.parse(string);
    ConnectorsBean retrievedBean = new ConnectorsBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(connectors.size(), retrievedBean.getConnectors().size());
    assertEquals(connectors.get(0), retrievedBean.getConnectors().get(0));
    assertEquals(connectors.get(1), retrievedBean.getConnectors().get(1));
  }

  @Test
  public void testNoDirection() {
    // Create testing connector
    List<MConnector> connectors = new LinkedList<MConnector>();
    connectors.add(BeanTestUtil.getConnector(1L, "jdbc", false, false));
    connectors.add(BeanTestUtil.getConnector(2L, "mysql", false, false));

    // Create testing bundles
    Map<Long, ResourceBundle> bundles = new HashMap<Long, ResourceBundle>();
    bundles.put(1L, ConfigTestUtil.getResourceBundle());
    bundles.put(2L, ConfigTestUtil.getResourceBundle());

    // Serialize it to JSON object
    ConnectorsBean bean = new ConnectorsBean(connectors, bundles);
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = JSONUtils.parse(string);
    ConnectorsBean retrievedBean = new ConnectorsBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(connectors.size(), retrievedBean.getConnectors().size());
    assertEquals(connectors.get(0), retrievedBean.getConnectors().get(0));
    assertEquals(connectors.get(1), retrievedBean.getConnectors().get(1));
  }

}

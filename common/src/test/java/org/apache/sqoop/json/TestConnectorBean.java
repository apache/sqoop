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

public class TestConnectorBean {

  /**
   * Test that by JSON serialization followed by deserialization we will get
   * equal connector object.
   */
  @Test
  public void testConnectorSerialization() {
    // Create testing connector
    List<MConnector> connectors = new LinkedList<MConnector>();
    connectors.add(BeanTestUtil.getConnector(1L, "jdbc"));

    // Create testing bundles
    Map<Long, ResourceBundle> configBundles = new HashMap<Long, ResourceBundle>();
    configBundles.put(1L, ConfigTestUtil.getResourceBundle());

    // Serialize it to JSON object
    ConnectorBean connectorBean = new ConnectorBean(connectors, configBundles);
    JSONObject connectorJSON = connectorBean.extract(false);

    // "Move" it across network in text form
    String connectorJSONString = connectorJSON.toJSONString();

    // Retrieved transferred object
    JSONObject parsedConnectors = JSONUtils.parse(connectorJSONString);
    ConnectorBean parsedConnectorBean = new ConnectorBean();
    parsedConnectorBean.restore(parsedConnectors);
    assertEquals(connectors.size(), 1);
    assertEquals(connectors.size(), parsedConnectorBean.getConnectors().size());
    assertEquals(connectors.get(0), parsedConnectorBean.getConnectors().get(0));
    ResourceBundle retrievedBundle = parsedConnectorBean.getResourceBundles().get(1L);
    assertNotNull(retrievedBundle);
    assertEquals("a", retrievedBundle.getString("a"));
    assertEquals("b", retrievedBundle.getString("b"));
  }
}

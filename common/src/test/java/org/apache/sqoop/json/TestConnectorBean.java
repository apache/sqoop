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

import static org.junit.Assert.*;

import org.apache.sqoop.model.MConnector;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import static org.apache.sqoop.json.TestUtil.*;

/**
 *
 */
public class TestConnectorBean {

//  /**
//   * Test that by JSON serialization followed by deserialization we will get
//   * equal connector object.
//   */
//  @Test
//  public void testSerialization() {
//    // Create testing connector
//    List<MConnector> connectors = new LinkedList<MConnector>();
//    connectors.add(getConnector("jdbc"));
//    connectors.add(getConnector("mysql"));
//
//    // Create testing bundles
//    Map<Long, ResourceBundle> bundles = new HashMap<Long, ResourceBundle>();
//    bundles.put(1L, getResourceBundle());
//    bundles.put(2L, getResourceBundle());
//
//    // Serialize it to JSON object
//    ConnectorBean bean = new ConnectorBean(connectors, bundles);
//    JSONObject json = bean.extract(false);
//
//    // "Move" it across network in text form
//    String string = json.toJSONString();
//
//    // Retrieved transferred object
//    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
//    ConnectorBean retrievedBean = new ConnectorBean();
//    retrievedBean.restore(retrievedJson);
//
//    assertEquals(connectors.size(), retrievedBean.getConnectors().size());
//    assertEquals(connectors.get(0), retrievedBean.getConnectors().get(0));
//
//    ResourceBundle retrievedBundle = retrievedBean.getResourceBundles().get(1L);
//    assertNotNull(retrievedBundle);
//    assertEquals("a", retrievedBundle.getString("a"));
//    assertEquals("b", retrievedBundle.getString("b"));
//  }
}

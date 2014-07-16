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

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.Date;

import static junit.framework.Assert.*;
import static org.apache.sqoop.json.TestUtil.*;

/**
 *
 */
public class TestConnectionBean {
//  @Test
//  public void testSerialization() {
//    Date created = new Date();
//    Date updated = new Date();
//    MConnection connection = getConnection("ahoj");
//    connection.setName("Connection");
//    connection.setPersistenceId(666);
//    connection.setCreationUser("admin");
//    connection.setCreationDate(created);
//    connection.setLastUpdateUser("user");
//    connection.setLastUpdateDate(updated);
//    connection.setEnabled(false);
//
//    // Fill some data at the beginning
//    MStringInput input = (MStringInput) connection.getConnectorPart().getForms()
//      .get(0).getInputs().get(0);
//    input.setValue("Hi there!");
//
//    // Serialize it to JSON object
//    ConnectionBean bean = new ConnectionBean(connection);
//    JSONObject json = bean.extract(false);
//
//    // Check for sensitivity
//    JSONArray all = (JSONArray)json.get("all");
//    JSONObject allItem = (JSONObject)all.get(0);
//    JSONArray connectors = (JSONArray)allItem.get("connector");
//    JSONObject connector = (JSONObject)connectors.get(0);
//    JSONArray inputs = (JSONArray)connector.get("inputs");
//    for (Object input1 : inputs) {
//      assertTrue(((JSONObject)input1).containsKey("sensitive"));
//    }
//
//    // "Move" it across network in text form
//    String string = json.toJSONString();
//
//    // Retrieved transferred object
//    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
//    ConnectionBean retrievedBean = new ConnectionBean();
//    retrievedBean.restore(retrievedJson);
//    MConnection target = retrievedBean.getConnections().get(0);
//
//    // Check id and name
//    assertEquals(666, target.getPersistenceId());
//    assertEquals("Connection", target.getName());
//    assertEquals("admin", target.getCreationUser());
//    assertEquals(created, target.getCreationDate());
//    assertEquals("user", target.getLastUpdateUser());
//    assertEquals(updated, target.getLastUpdateDate());
//    assertEquals(false, target.getEnabled());
//
//    // Test that value was correctly moved
//    MStringInput targetInput = (MStringInput) target.getConnectorPart()
//      .getForms().get(0).getInputs().get(0);
//    assertEquals("Hi there!", targetInput.getValue());
//  }
//
//  @Test
//  public void testSensitivityFilter() {
//    Date created = new Date();
//    Date updated = new Date();
//    MConnection connection = getConnection("ahoj");
//    connection.setName("Connection");
//    connection.setPersistenceId(666);
//    connection.setCreationUser("admin");
//    connection.setCreationDate(created);
//    connection.setLastUpdateUser("user");
//    connection.setLastUpdateDate(updated);
//    connection.setEnabled(true);
//
//    // Fill some data at the beginning
//    MStringInput input = (MStringInput) connection.getConnectorPart().getForms()
//      .get(0).getInputs().get(0);
//    input.setValue("Hi there!");
//
//    // Serialize it to JSON object
//    ConnectionBean bean = new ConnectionBean(connection);
//    JSONObject json = bean.extract(false);
//    JSONObject jsonFiltered = bean.extract(true);
//
//    // Sensitive values should exist
//    JSONArray all = (JSONArray)json.get("all");
//    JSONObject allItem = (JSONObject)all.get(0);
//    JSONArray connectors = (JSONArray)allItem.get("connector");
//    JSONObject connector = (JSONObject)connectors.get(0);
//    JSONArray inputs = (JSONArray)connector.get("inputs");
//    assertEquals(3, inputs.size());
//    // Inputs are ordered when creating connection
//    JSONObject password = (JSONObject)inputs.get(2);
//    assertTrue(password.containsKey("value"));
//
//    // Sensitive values should not exist
//    all = (JSONArray)jsonFiltered.get("all");
//    allItem = (JSONObject)all.get(0);
//    connectors = (JSONArray)allItem.get("connector");
//    connector = (JSONObject)connectors.get(0);
//    inputs = (JSONArray)connector.get("inputs");
//    assertEquals(3, inputs.size());
//    // Inputs are ordered when creating connection
//    password = (JSONObject)inputs.get(2);
//    assertFalse(password.containsKey("value"));
//  }
}

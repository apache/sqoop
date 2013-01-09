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
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.Date;

import static junit.framework.Assert.*;
import static org.apache.sqoop.json.TestUtil.*;

/**
 *
 */
public class TestConnectionBean {
  @Test
  public void testSerialization() {
    Date created = new Date();
    Date updated = new Date();
    MConnection connection = getConnection("ahoj");
    connection.setName("Connection");
    connection.setPersistenceId(666);
    connection.setCreationDate(created);
    connection.setLastUpdateDate(updated);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) connection.getConnectorPart().getForms()
      .get(0).getInputs().get(0);
    input.setValue("Hi there!");

    // Serialize it to JSON object
    ConnectionBean bean = new ConnectionBean(connection);
    JSONObject json = bean.extract();

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    ConnectionBean retrievedBean = new ConnectionBean();
    retrievedBean.restore(retrievedJson);
    MConnection target = retrievedBean.getConnections().get(0);

    // Check id and name
    assertEquals(666, target.getPersistenceId());
    assertEquals("Connection", target.getName());
    assertEquals(created, target.getCreationDate());
    assertEquals(updated, target.getLastUpdateDate());

    // Test that value was correctly moved
    MStringInput targetInput = (MStringInput) target.getConnectorPart()
      .getForms().get(0).getInputs().get(0);
    assertEquals("Hi there!", targetInput.getValue());
  }
}

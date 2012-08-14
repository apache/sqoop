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

import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TestConnectorBean {

  /**
   * Test that by JSON serialization followed by deserialization we will get
   * equal connector object.
   */
  @Test
  public void testSerialization() {
    // Create testing connector
    MConnector[] connectors = new MConnector[2];
    connectors[0] = buildFakeConnector("jdbc");
    connectors[1] = buildFakeConnector("mysql");

    // Serialize it to JSON object
    ConnectorBean bean = new ConnectorBean(connectors);
    JSONObject json = bean.extract();

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    ConnectorBean retrievedBean = new ConnectorBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(connectors.length, retrievedBean.getConnectors().length);
    assertEquals(connectors[0], retrievedBean.getConnectors()[0]);
  }

  protected MConnector buildFakeConnector(String name) {
    List<MInput<?>> inputs;

    List<MForm> connectionForms = new ArrayList<MForm>();
    inputs = new ArrayList<MInput<?>>();
    inputs.add(new MStringInput("url", false, (short) 10));
    inputs.add(new MStringInput("username", false, (short) 10));
    inputs.add(new MStringInput("password", false, (short) 10));
    connectionForms.add(new MForm("connection", inputs));

    inputs = new ArrayList<MInput<?>>();
    inputs.add(new MMapInput("properties"));
    connectionForms.add(new MForm("properties", inputs));
    MConnection connection = new MConnection(connectionForms);

    List<MForm> jobForms = new ArrayList<MForm>();
    inputs = new ArrayList<MInput<?>>();
    inputs.add(new MStringInput("A", false, (short) 10));
    inputs.add(new MStringInput("B", false, (short) 10));
    inputs.add(new MStringInput("C", false, (short) 10));
    jobForms.add((new MForm("D", inputs)));

    inputs = new ArrayList<MInput<?>>();
    inputs.add(new MStringInput("Z", false, (short) 10));
    inputs.add(new MStringInput("X", false, (short) 10));
    inputs.add(new MStringInput("Y", false, (short) 10));
    jobForms.add(new MForm("D", inputs));

    List<MJob> jobs = new ArrayList<MJob>();
    jobs.add(new MJob(MJob.Type.IMPORT, jobForms));

    return new MConnector(name, name + ".class", connection, jobs);
  }
}

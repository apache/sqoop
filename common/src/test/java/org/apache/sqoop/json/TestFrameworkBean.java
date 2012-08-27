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

import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 */
public class TestFrameworkBean {

  /**
   * Test that by JSON serialization followed by deserialization we will get
   * equal framework object.
   */
  @Test
  public void testSerialization() {
    MFramework framework = getFramework("1");
    // Serialize it to JSON object
    FrameworkBean bean = new FrameworkBean(framework);
    JSONObject json = bean.extract();

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject) JSONValue.parse(string);
    FrameworkBean retrievedBean = new FrameworkBean();
    retrievedBean.restore(retrievedJson);

    assertEquals(framework, retrievedBean.getFramework());
  }

  public MFramework getFramework(String parameter) {
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
    MConnectionForms connection = new MConnectionForms(connectionForms);

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

    List<MJobForms> jobs = new ArrayList<MJobForms>();
    jobs.add(new MJobForms(MJob.Type.IMPORT, jobForms));

    return new MFramework(connection, jobs);
  }

}

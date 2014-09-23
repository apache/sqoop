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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.utils.MapResourceBundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 */
public class TestUtil {
  public static MConnector getConnector(String name) {
    return new MConnector(name, name + ".class", "1.0-test",
      getConnectionForms(), getJobForms(), getJobForms());
  }

  public static MDriverConfig getDriverConfig() {
    return new MDriverConfig(getConnectionForms(), getJobForms(), "1");
  }

  public static MLink getLink(String name) {
    return new MLink(1, getConnector(name).getConnectionForms(), getDriverConfig()
        .getConnectionForms());
  }

  public static MJob getJob(String name) {
    return new MJob(1, 2, 1, 2, getConnector(name).getJobForms(Direction.FROM), getConnector(name)
        .getJobForms(Direction.TO), getDriverConfig().getJobForms());
  }

  public static MConnectionForms getConnectionForms() {
    List<MInput<?>> inputs;
    MStringInput input;
    MForm form;
    List<MForm> connectionForms = new ArrayList<MForm>();
    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("url", false, (short) 10);
    input.setPersistenceId(1);
    inputs.add(input);

    input = new MStringInput("username", false, (short) 10);
    input.setPersistenceId(2);
    input.setValue("test");
    inputs.add(input);

    input = new MStringInput("password", true, (short) 10);
    input.setPersistenceId(3);
    input.setValue("test");
    inputs.add(input);

    form = new MForm("connection", inputs);
    form.setPersistenceId(10);
    connectionForms.add(form);

    return new MConnectionForms(connectionForms);
  }

  public static MJobForms getJobForms() {
    List<MInput<?>> inputs;
    MStringInput input;
    MForm form;
    List<MForm> jobForms = new ArrayList<MForm>();

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("A", false, (short) 10);
    input.setPersistenceId(4);
    inputs.add(input);

    input = new MStringInput("B", false, (short) 10);
    input.setPersistenceId(5);
    inputs.add(input);

    input = new MStringInput("C", false, (short) 10);
    input.setPersistenceId(6);
    inputs.add(input);

    form = new MForm("Z", inputs);
    form.setPersistenceId(11);
    jobForms.add(form);

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("D", false, (short) 10);
    input.setPersistenceId(7);
    inputs.add(input);

    input = new MStringInput("E", false, (short) 10);
    input.setPersistenceId(8);
    inputs.add(input);

    input = new MStringInput("F", false, (short) 10);
    input.setPersistenceId(9);
    inputs.add(input);

    form = new MForm("connection", inputs);
    form.setPersistenceId(12);
    jobForms.add(form);

    return new MJobForms(jobForms);
  }

  public static ResourceBundle getResourceBundle() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("a", "a");
    map.put("b", "b");

    return new MapResourceBundle(map);
  }
}

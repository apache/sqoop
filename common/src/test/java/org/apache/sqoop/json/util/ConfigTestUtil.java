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
package org.apache.sqoop.json.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.utils.MapResourceBundle;

public class ConfigTestUtil {

  public static MDriverConfig getDriverConfig() {
    List<MInput<?>> inputs;
    MIntegerInput input;
    MConfig config;
    List<MConfig> driverConfigs = new ArrayList<MConfig>();
    inputs = new ArrayList<MInput<?>>();

    input = new MIntegerInput("numExtractors", false, InputEditable.ANY, StringUtils.EMPTY);
    input.setPersistenceId(1);
    inputs.add(input);

    input = new MIntegerInput("numLoaders", false, InputEditable.USER_ONLY, StringUtils.EMPTY);
    input.setPersistenceId(2);
    inputs.add(input);

    config = new MConfig("driver", inputs);
    config.setPersistenceId(10);
    driverConfigs.add(config);
    return new MDriverConfig(driverConfigs);
  }

  public static MLinkConfig getLinkConfig() {
    List<MInput<?>> inputs;
    MStringInput input;
    MConfig config;
    List<MConfig> linkConfig = new ArrayList<MConfig>();
    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("url", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(1);
    inputs.add(input);

    input = new MStringInput("username", false, InputEditable.USER_ONLY, "password", (short) 10);
    input.setPersistenceId(2);
    input.setValue("test");
    inputs.add(input);

    input = new MStringInput("password", true, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(3);
    input.setValue("test");
    inputs.add(input);

    config = new MConfig("connection", inputs);
    config.setPersistenceId(10);
    linkConfig.add(config);

    return new MLinkConfig(linkConfig);
  }

  static MFromConfig getFromConfig() {
    List<MInput<?>> inputs;
    MStringInput input;
    MConfig config;
    List<MConfig> jobConfigs = new ArrayList<MConfig>();

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("A", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(4);
    inputs.add(input);

    input = new MStringInput("B", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(5);
    inputs.add(input);

    input = new MStringInput("C", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(6);
    inputs.add(input);

    config = new MConfig("Z", inputs);
    config.setPersistenceId(11);
 jobConfigs.add(config);

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("D", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(7);
    inputs.add(input);

    input = new MStringInput("E", false, InputEditable.USER_ONLY, "D, F", (short) 10);
    input.setPersistenceId(8);
    inputs.add(input);

    input = new MStringInput("F", false, InputEditable.USER_ONLY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(9);
    inputs.add(input);

    config = new MConfig("from-table", inputs);
    config.setPersistenceId(12);
    jobConfigs.add(config);

    return new MFromConfig(jobConfigs);
  }

  static MToConfig getToConfig() {
    List<MInput<?>> inputs;
    MStringInput input;
    MConfig config;
    List<MConfig> jobConfigs = new ArrayList<MConfig>();

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("A", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(4);
    inputs.add(input);

    input = new MStringInput("B", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(5);
    inputs.add(input);

    input = new MStringInput("C", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(6);
    inputs.add(input);

    config = new MConfig("Z", inputs);
    config.setPersistenceId(11);
    jobConfigs.add(config);

    inputs = new ArrayList<MInput<?>>();

    input = new MStringInput("D", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(7);
    inputs.add(input);

    input = new MStringInput("E", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(8);
    inputs.add(input);

    input = new MStringInput("F", false, InputEditable.ANY, StringUtils.EMPTY, (short) 10);
    input.setPersistenceId(9);
 inputs.add(input);

    config = new MConfig("to-table", inputs);
    config.setPersistenceId(12);
    jobConfigs.add(config);

    return new MToConfig(jobConfigs);
  }

  public static ResourceBundle getResourceBundle() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("a", "a");
    map.put("b", "b");

    return new MapResourceBundle(map);
  }
}

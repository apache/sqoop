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
package org.apache.sqoop.model;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class TestMConfigList {
  @Test
  public void testGetInputs() {
    List<MConfig> configs = new LinkedList<MConfig>();

    MIntegerInput intInput = new MIntegerInput("Config1.A", false, InputEditable.ANY, StringUtils.EMPTY);
    MMapInput mapInput = new MMapInput("Config1.B", false, InputEditable.ANY, StringUtils.EMPTY);

    List<MInput<?>> inputs = new ArrayList<MInput<?>>();
    inputs.add(intInput);
    inputs.add(mapInput);
    configs.add(new MConfig("Config1", inputs));

    MStringInput stringInput = new MStringInput("Config2.C", false, InputEditable.ANY,
        StringUtils.EMPTY, (short) 3);
    MEnumInput enumInput = new MEnumInput("Config2.D", false, InputEditable.ANY, StringUtils.EMPTY,
        new String[] { "I", "V" });

    inputs = new ArrayList<MInput<?>>();
    inputs.add(stringInput);
    inputs.add(enumInput);
    configs.add(new MConfig("Config2", inputs));

    MConfigList config = new MConfigList(configs, MConfigType.JOB);
    assertEquals(intInput, config.getIntegerInput("Config1.A"));
    assertEquals(mapInput, config.getMapInput("Config1.B"));
    assertEquals(stringInput, config.getStringInput("Config2.C"));
    assertEquals(enumInput, config.getEnumInput("Config2.D"));
  }
}

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestMConfig {

  /**
   * Test for initialization
   */
  @Test
  public void testInitialization() {
    MInput<String> input1 = new MStringInput("sqoopsqoop1", true, InputEditable.ANY, StringUtils.EMPTY , (short) 5, Collections.EMPTY_LIST);
    MInput<String> input2 = new MStringInput("sqoopsqoop2", true, InputEditable.ANY, StringUtils.EMPTY , (short) 5, Collections.EMPTY_LIST);

    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input1);
    list.add(input2);
    MConfig mConfig = new MConfig("config", list, Collections.EMPTY_LIST);

    assertEquals("config", mConfig.getName());
    assertEquals(2, mConfig.getInputs().size());
  }

  /**
   * Test for equals method
   */
  @Test
  public void testEquals() {
    MInput<Integer> input1 = new MIntegerInput("sqoopsqoop1", false, InputEditable.ANY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    MInput<Integer> input2 = new MIntegerInput("sqoopsqoop2", false, InputEditable.ANY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    List<MInput<?>> list1 = new ArrayList<MInput<?>>();
    list1.add(input1);
    list1.add(input2);
    MConfig mConfig1 = new MConfig("config", list1, Collections.EMPTY_LIST);

    MInput<Integer> input3 = new MIntegerInput("sqoopsqoop1", false, InputEditable.ANY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    MInput<Integer> input4 = new MIntegerInput("sqoopsqoop2", false, InputEditable.ANY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    List<MInput<?>> list2 = new ArrayList<MInput<?>>();
    list2.add(input3);
    list2.add(input4);
    MConfig mConfig2 = new MConfig("config", list2, Collections.EMPTY_LIST);
    assertEquals(mConfig2, mConfig1);
  }

  @Test
  public void testGetInputs() {
    MIntegerInput intInput = new MIntegerInput("Config.A", false, InputEditable.ANY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    MLongInput longInput = new MLongInput("Config.A1", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST );
    MMapInput mapInput = new MMapInput("Config.B", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    MStringInput stringInput = new MStringInput("Config.C", false, InputEditable.ANY,
        StringUtils.EMPTY, (short) 3, Collections.EMPTY_LIST);
    MEnumInput enumInput = new MEnumInput("Config.D", false, InputEditable.ANY, StringUtils.EMPTY,
        new String[] { "I", "V" }, Collections.EMPTY_LIST);
    MListInput listInput = new MListInput("Config.E", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST );
    MDateTimeInput dtInput = new MDateTimeInput("Config.F", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST );

    List<MInput<?>> inputs = new ArrayList<MInput<?>>();
    inputs.add(intInput);
    inputs.add(longInput);
    inputs.add(mapInput);
    inputs.add(stringInput);
    inputs.add(enumInput);
    inputs.add(listInput);
    inputs.add(dtInput);

    MConfig config = new MConfig("Config", inputs, Collections.EMPTY_LIST);
    assertEquals(intInput, config.getIntegerInput("Config.A"));
    assertEquals(longInput, config.getLongInput("Config.A1"));
    assertEquals(mapInput, config.getMapInput("Config.B"));
    assertEquals(stringInput, config.getStringInput("Config.C"));
    assertEquals(enumInput, config.getEnumInput("Config.D"));
    assertEquals(listInput, config.getListInput("Config.E"));
    assertEquals(dtInput, config.getDateTimeInput("Config.F"));
  }
}

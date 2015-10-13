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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestMLinkConfig {

  /**
   * Test for class initialization and values
   */
  @Test
  public void testInitialization() {
    List<MConfig> configs = new ArrayList<>();
    List<MValidator> validators = new ArrayList<>();
    MLinkConfig linkConfig = new MLinkConfig(configs, validators);
    List<MConfig> testConfig = new ArrayList<>();
    List<MValidator> testValidator = new ArrayList<>();
    assertEquals(testConfig, linkConfig.getConfigs());
    MLinkConfig linkConfig2 = new MLinkConfig(testConfig, testValidator);
    assertEquals(linkConfig2, linkConfig);
    // Add a config to list for checking not equals
    MConfig c = new MConfig("test", null, Collections.EMPTY_LIST);
    testConfig.add(c);
    assertFalse(linkConfig.equals(linkConfig2));
  }
}

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test class for org.apache.sqoop.model.MMapInput
 */
public class TestMMapInput {
  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    MMapInput input = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertEquals("sqoopsqoop", input.getName());
    assertEquals(MInputType.MAP, input.getType());
  }

  /**
   * Test for equals() method
   */
  @Test
  public void testEquals() {
    // Positive test
    MMapInput input1 = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    MMapInput input2 = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertTrue(input1.equals(input2));

    // Negative test
    MMapInput input3 = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    MMapInput input4 = new MMapInput("sqoopsqoop1", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(input3.equals(input4));
  }

  /**
   * Test for value
   */
  @Test
  public void testValue() {
    MMapInput input1 = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    Map<String, String> map1 = new HashMap<String, String>();
    input1.setValue(map1);
    assertEquals(map1, input1.getValue());
    input1.setEmpty();
    assertNull(input1.getValue());
  }

  /**
   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
   */
  @Test
  public void testUrlSafe() {
    MMapInput input1 = new MMapInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    Map<String, String> map1 = new HashMap<String, String>();
    input1.setValue(map1);
    // Getting URL safe string
    String tmp = input1.getUrlSafeValueString();
    // Restore to actual value
    input1.restoreFromUrlSafeValueString(tmp);
    assertNotNull(input1.getValue());
    assertEquals(0, input1.getValue().size());

    input1.setValue(null);
    tmp = input1.getUrlSafeValueString();
    input1.restoreFromUrlSafeValueString(tmp);
    assertNull(input1.getValue());
  }

  /**
   * Test case for MNamedElement.getLabelKey() and MNamedElement.getHelpKey()
   */
  @Test
  public void testNamedElement() {
    MMapInput input1 = new MMapInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertEquals("sqoopsqoop.label", input1.getLabelKey());
    assertEquals("sqoopsqoop.help", input1.getHelpKey());
  }

  /**
   * Test for sensitivity
   */
  @Test
  public void testSensitivity() {
    MMapInput input1 = new MMapInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    MMapInput input2 = new MMapInput("NAME", true, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    assertFalse(input1.isSensitive());
    assertTrue(input2.isSensitive());
  }

  /**
   * Test for sensitivity
   */
  @Test
  public void testSensitiveKeyPattern() {
    Map<String, String> testValue = new HashMap<>();
    testValue.put("sqoop features", "awesome features");
    testValue.put("sqoop bugs", "horrible bugs");

    MMapInput input1 = new MMapInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY , Collections.EMPTY_LIST);
    input1.setValue(testValue);
    MMapInput input2 = new MMapInput("NAME", true, InputEditable.ANY, StringUtils.EMPTY, ".*bugs.*", Collections.EMPTY_LIST);
    input2.setValue(testValue);

    assertEquals(input1.getNonsenstiveValue(), testValue);

    Map<String, String> expectedNonsensitiveMap = new HashMap<>();
    expectedNonsensitiveMap.put("sqoop features", "awesome features");
    expectedNonsensitiveMap.put("sqoop bugs", MMapInput.SENSITIVE_VALUE_PLACEHOLDER);
    assertEquals(input2.getNonsenstiveValue(), expectedNonsensitiveMap);
  }
}

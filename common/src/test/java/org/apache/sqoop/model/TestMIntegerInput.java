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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test class for org.apache.sqoop.model.MInputInput
 */
public class TestMIntegerInput {
  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    MIntegerInput input = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    assertEquals("sqoopsqoop", input.getName());
    assertEquals(MInputType.INTEGER, input.getType());
  }

  /**
   * Test for equals() method
   */
  @Test
  public void testEquals() {
    // Positive test
    MIntegerInput input1 = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    MIntegerInput input2 = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    assertTrue(input1.equals(input2));

    // Negative test
    MIntegerInput input3 = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    MIntegerInput input4 = new MIntegerInput("sqoopsqoop1", false, InputEditable.ANY, StringUtils.EMPTY);
    assertFalse(input3.equals(input4));
  }

  /**
   * Test for value
   */
  @Test
  public void testValue() {
    MIntegerInput input1 = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    input1.setValue(99);
    assertEquals(new Integer(99), input1.getValue());
    input1.setEmpty();
    assertNull(input1.getValue());
  }

  /**
   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
   */
  @Test
  public void testUrlSafe() {
    MIntegerInput input1 = new MIntegerInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    input1.setValue(1001);
    // Getting URL safe string
    String tmp = input1.getUrlSafeValueString();
    // Restore to actual value
    input1.restoreFromUrlSafeValueString(tmp);
    assertEquals(new Integer(1001), input1.getValue());
  }

  /**
   * Test case for MNamedElement.getLabelKey() and MNamedElement.getHelpKey()
   */
  @Test
  public void testNamedElement() {
    MStringInput input1 = new MStringInput("sqoopsqoop", true, InputEditable.CONNECTOR_ONLY, StringUtils.EMPTY, (short) 5);
    assertEquals("sqoopsqoop.label", input1.getLabelKey());
    assertEquals("sqoopsqoop.help", input1.getHelpKey());
  }

  /**
   * Test for sensitivity
   */
  @Test
  public void testSensitivity() {
    MIntegerInput input1 = new MIntegerInput("NAME", false, InputEditable.USER_ONLY, StringUtils.EMPTY);
    MIntegerInput input2 = new MIntegerInput("NAME", true, InputEditable.CONNECTOR_ONLY, StringUtils.EMPTY);
    assertFalse(input1.isSensitive());
    assertTrue(input2.isSensitive());
  }
}

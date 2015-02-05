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

import static org.testng.Assert.*;

/**
 * Test class for org.apache.sqoop.model.MStringInput
 */
public class TestMStringInput {

  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    short len = 6;
    MStringInput input = new MStringInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY,
        len);
    assertEquals("sqoopsqoop", input.getName());
    assertEquals(true, input.isSensitive());
    assertEquals(len, input.getMaxLength());
    assertEquals(MInputType.STRING, input.getType());
  }

  /**
   * Test for equals() method
   */
  @Test
  public void testEquals() {
    short len = 6;
    // Positive test
    MStringInput input1 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, len);
    MStringInput input2 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, len);
    assertTrue(input1.equals(input2));

    // Negative test
    MStringInput input3 = new MStringInput("sqoopsqoop", false, InputEditable.ANY,
        StringUtils.EMPTY, len);
    MStringInput input4 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, len);
    assertFalse(input3.equals(input4));
  }

  /**
   * Test for value
   */
  @Test
  public void testValue() {
    MStringInput input1 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, (short) 5);
    input1.setValue("sqoop");
    assertEquals("sqoop", input1.getValue());
    input1.setEmpty();
    assertNull(input1.getValue());
  }

  /**
   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
   */
  @Test
  public void testUrlSafe() {
    MStringInput input1 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, (short) 5);
    String s = "Sqoop%$!@#&*()Sqoop";
    input1.setValue(s);
    // Getting URL safe string
    String tmp = input1.getUrlSafeValueString();
    // Restore to actual value
    input1.restoreFromUrlSafeValueString(tmp);
    assertEquals(s, input1.getValue());
  }

  /**
   * Test case for MNamedElement.getLabelKey() and MNamedElement.getHelpKey()
   */
  @Test
  public void testNamedElement() {
    MStringInput input1 = new MStringInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY, (short) 5);
    assertEquals("sqoopsqoop.label", input1.getLabelKey());
    assertEquals("sqoopsqoop.help", input1.getHelpKey());
  }
}

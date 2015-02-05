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
 * Test class for org.apache.sqoop.model.MBooleanInput
 */
public class TestMBooleanInput {

  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    MBooleanInput input = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY);
    assertEquals("sqoopsqoop", input.getName());
    assertEquals(true, input.isSensitive());
    assertEquals(MInputType.BOOLEAN, input.getType());
  }

  /**
   * Test for equals() method
   */
  @Test
  public void testEquals() {
    // Positive test
    MBooleanInput input1 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY);
    MBooleanInput input2 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY);
    assertTrue(input1.equals(input2));

    // Negative test
    MBooleanInput input3 = new MBooleanInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY );
    MBooleanInput input4 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY );
    assertFalse(input3.equals(input4));

    MBooleanInput input5 = new MBooleanInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY );
    MBooleanInput input6 = new MBooleanInput("sqoop", false, InputEditable.ANY, StringUtils.EMPTY );
    assertFalse(input5.equals(input6));
  }

  /**
   * Test for value
   */
  @Test
  public void testValue() {
    MBooleanInput input1 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY );
    input1.setValue(true);
    assertEquals(true, input1.getValue().booleanValue());
    input1.setEmpty();
    assertNull(input1.getValue());
  }

  /**
   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
   */
  @Test
  public void testUrlSafe() {
    MBooleanInput input1 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY );
    input1.setValue(true);
    // Getting URL safe string
    String tmp = input1.getUrlSafeValueString();
    // Restore to actual value
    input1.restoreFromUrlSafeValueString(tmp);
    assertEquals(true, input1.getValue().booleanValue());
  }

  /**
   * Test case for MNamedElement.getLabelKey() and MNamedElement.getHelpKey()
   */
  @Test
  public void testNamedElement() {
    MBooleanInput input1 = new MBooleanInput("sqoopsqoop", true, InputEditable.ANY,
        StringUtils.EMPTY);
    assertEquals("sqoopsqoop.label", input1.getLabelKey());
    assertEquals("sqoopsqoop.help", input1.getHelpKey());
  }
}

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

/**
 * Test class for org.apache.sqoop.model.MDateTimeInput
 */
public class TestMDateTimeInput {
  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    MDateTimeInput input = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    assertEquals("sqoopsqoop", input.getName());
    assertEquals(MInputType.DATETIME, input.getType());
  }

  /**
   * Test for equals() method
   */
  @Test
  public void testEquals() {
    // Positive test
    MDateTimeInput input1 = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    MDateTimeInput input2 = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    assertTrue(input1.equals(input2));

    // Negative test
    MDateTimeInput input3 = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    MDateTimeInput input4 = new MDateTimeInput("sqoopsqoop1", false, InputEditable.ANY, StringUtils.EMPTY);
    assertFalse(input3.equals(input4));
  }

  /**
   * Test for value
   */
  @Test
  public void testValue() {
    MDateTimeInput input1 = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);

    //  Test for long format
    DateTime dt = new DateTime(1234567L);
    input1.setValue(dt);
    assertEquals(dt, input1.getValue());

    // Test for ISO8601 format
    dt = DateTime.parse("2010-06-30T01:20");
    input1.setValue(dt);
    assertEquals(dt, input1.getValue());

    input1.setEmpty();
    assertNull(input1.getValue());
  }

  /**
   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
   */
  @Test
  public void testUrlSafe() {
    MDateTimeInput input1 = new MDateTimeInput("sqoopsqoop", false, InputEditable.ANY, StringUtils.EMPTY);
    DateTime dt = new DateTime(1234567L);
    input1.setValue(dt);
    // Getting URL safe string
    String tmp = input1.getUrlSafeValueString();
    // Restore to actual value
    input1.restoreFromUrlSafeValueString(tmp);
    assertNotNull(input1.getValue());
    assertEquals(dt, input1.getValue());

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
    MDateTimeInput input1 = new MDateTimeInput("sqoopsqoop", true, InputEditable.ANY, StringUtils.EMPTY);
    assertEquals("sqoopsqoop.label", input1.getLabelKey());
    assertEquals("sqoopsqoop.help", input1.getHelpKey());
  }

  /**
   * Test for sensitivity
   */
  @Test
  public void testSensitivity() {
    MDateTimeInput input1 = new MDateTimeInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY );
    MDateTimeInput input2 = new MDateTimeInput("NAME", true, InputEditable.ANY, StringUtils.EMPTY );
    assertFalse(input1.isSensitive());
    assertTrue(input2.isSensitive());
  }
}

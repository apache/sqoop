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

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test class for org.apache.sqoop.model.MEnumInput
 */
public class TestMEnumInput {

  public enum Enumeration { value1, value2}
  /**
   * Test for class initialization
   */
  @Test
  public void testInitialization() {
    String[] values = { "value1", "value2" };
    MEnumInput input = new MEnumInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, values, Collections.EMPTY_LIST);
    assertEquals("NAME", input.getName());
    assertEquals(values, input.getValues());
    assertEquals(MInputType.ENUM, input.getType());

    MEnumInput input1 = new MEnumInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, values, Collections.EMPTY_LIST);
    assertEquals(input1, input);
    String[] testVal = { "val", "test" };
    MEnumInput input2 = new MEnumInput("NAME1", false, InputEditable.ANY, StringUtils.EMPTY,
        testVal, Collections.EMPTY_LIST);
    assertFalse(input1.equals(input2));

    MEnumInput input3 = new MEnumInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, values, Collections.EMPTY_LIST);
    input3.setValue(Enumeration.value1);
    assertEquals("value1", input3.getValue());
  }

  /**
   * Test for sensitivity
   */
  @Test
  public void testSensitivity() {
    String[] values = { "value1", "value2" };
    MEnumInput input1 = new MEnumInput("NAME", false, InputEditable.ANY, StringUtils.EMPTY, values, Collections.EMPTY_LIST);
    MEnumInput input2 = new MEnumInput("NAME", true, InputEditable.ANY, StringUtils.EMPTY, values, Collections.EMPTY_LIST);
    assertFalse(input1.isSensitive());
    assertTrue(input2.isSensitive());
  }
}

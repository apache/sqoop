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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.MMapInput
 */
public class TestMMapInput {
//  /**
//   * Test for class initialization
//   */
//  @Test
//  public void testInitialization() {
//    MMapInput input = new MMapInput("sqoopsqoop", false);
//    assertEquals("sqoopsqoop", input.getName());
//    assertEquals(MInputType.MAP, input.getType());
//  }
//
//  /**
//   * Test for equals() method
//   */
//  @Test
//  public void testEquals() {
//    // Positive test
//    MMapInput input1 = new MMapInput("sqoopsqoop", false);
//    MMapInput input2 = new MMapInput("sqoopsqoop", false);
//    assertTrue(input1.equals(input2));
//
//    // Negative test
//    MMapInput input3 = new MMapInput("sqoopsqoop", false);
//    MMapInput input4 = new MMapInput("sqoopsqoop1", false);
//    assertFalse(input3.equals(input4));
//  }
//
//  /**
//   * Test for value
//   */
//  @Test
//  public void testValue() {
//    MMapInput input1 = new MMapInput("sqoopsqoop", false);
//    Map<String, String> map1 = new HashMap<String, String>();
//    input1.setValue(map1);
//    assertEquals(map1, input1.getValue());
//    input1.setEmpty();
//    assertNull(input1.getValue());
//  }
//
//  /**
//   * Test for getUrlSafeValueString() and restoreFromUrlSafeValueString()
//   */
//  @Test
//  public void testUrlSafe() {
//    MMapInput input1 = new MMapInput("sqoopsqoop", false);
//    Map<String, String> map1 = new HashMap<String, String>();
//    input1.setValue(map1);
//    // Getting URL safe string
//    String tmp = input1.getUrlSafeValueString();
//    // Restore to actual value
//    input1.restoreFromUrlSafeValueString(tmp);
//    assertNotNull(input1.getValue());
//    assertEquals(0, input1.getValue().size());
//
//    input1.setValue(null);
//    tmp = input1.getUrlSafeValueString();
//    input1.restoreFromUrlSafeValueString(tmp);
//    assertNull(input1.getValue());
//  }
//
//  /**
//   * Test case for MNamedElement.getLabelKey() and MNamedElement.getHelpKey()
//   */
//  @Test
//  public void testNamedElement() {
//    MStringInput input1 = new MStringInput("sqoopsqoop", true, (short) 5);
//    assertEquals("sqoopsqoop.label", input1.getLabelKey());
//    assertEquals("sqoopsqoop.help", input1.getHelpKey());
//  }
//
//  /**
//   * Test for sensitivity
//   */
//  @Test
//  public void testSensitivity() {
//    MMapInput input1 = new MMapInput("NAME", false);
//    MMapInput input2 = new MMapInput("NAME", true);
//    assertFalse(input1.isSensitive());
//    assertTrue(input2.isSensitive());
//  }
}

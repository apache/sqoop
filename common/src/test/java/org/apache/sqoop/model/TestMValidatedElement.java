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

import static org.junit.Assert.*;

import org.apache.sqoop.validation.Status;
import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.MValidatedElement
 */
public class TestMValidatedElement {

//  /**
//   * Test for initalization
//   */
//  @Test
//  public void testInitialization() {
//    MValidatedElement input = new MIntegerInput("input", false);
//    assertEquals("input", input.getName());
//    assertEquals(Status.FINE, input.getValidationStatus());
//  }
//
//  /**
//   * Test for validation message and status
//   */
//  @Test
//  public void testValidationMessageStatus() {
//    MValidatedElement input = new MIntegerInput("input", false);
//    // Default status
//    assertEquals(Status.FINE, input.getValidationStatus());
//    // Set status and user message
//    input.setValidationMessage(Status.ACCEPTABLE, "MY_MESSAGE");
//    assertEquals(Status.ACCEPTABLE, input.getValidationStatus());
//    assertEquals("MY_MESSAGE", input.getValidationMessage());
//    // Check for null if status does not equal
//    assertNull(input.getValidationMessage(Status.FINE));
//    assertNull(input.getErrorMessage());
//    assertNotNull(input.getWarningMessage());
//    // Set unacceptable status
//    input.setValidationMessage(Status.UNACCEPTABLE, "MY_MESSAGE");
//    assertNotNull(input.getErrorMessage());
//    assertEquals("MY_MESSAGE", input.getErrorMessage());
//    assertNull(input.getWarningMessage());
//    // Set warning
//    input.setWarningMessage("WARN");
//    assertEquals(Status.ACCEPTABLE, input.getValidationStatus());
//    assertEquals("WARN", input.getValidationMessage());
//    // Unacceptable method
//    input.setErrorMessage("ERROR");
//    assertEquals(Status.UNACCEPTABLE, input.getValidationStatus());
//    assertEquals("ERROR", input.getValidationMessage());
//  }
}

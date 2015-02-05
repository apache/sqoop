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
import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Test class for org.apache.sqoop.model.MValidatedElement
 */
public class TestMValidatedElement {

  /**
   * Test for initalization
   */
  @Test
  public void testInitialization() {
    MValidatedElement input = new MIntegerInput("input", false,InputEditable.ANY, StringUtils.EMPTY );
    assertEquals("input", input.getName());
    assertEquals(Status.OK, input.getValidationStatus());
  }

  /**
   * Test for validation message and status
   */
  @Test
  public void testVarious() {
    MValidatedElement input = new MIntegerInput("input", false, InputEditable.ANY, StringUtils.EMPTY );

    // Default status
    assertEquals(Status.OK, input.getValidationStatus());

    // Add a message
    input.addValidationMessage(new Message(Status.WARNING, "MY_MESSAGE"));
    assertEquals(Status.WARNING, input.getValidationStatus());
    assertEquals(1, input.getValidationMessages().size());
    assertEquals("MY_MESSAGE", input.getValidationMessages().get(0).getMessage());

    // Reset
    input.resetValidationMessages();
    assertEquals(Status.OK, input.getValidationStatus());
    assertEquals(0, input.getValidationMessages().size());

    // Set unacceptable status
    input.addValidationMessage(new Message(Status.ERROR, "MY_MESSAGE"));
    assertEquals(Status.ERROR, input.getValidationStatus());
    assertEquals(1, input.getValidationMessages().size());
    assertEquals("MY_MESSAGE", input.getValidationMessages().get(0).getMessage());
  }
}

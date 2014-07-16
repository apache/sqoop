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
package org.apache.sqoop.validation;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.validation.Validation.FormInput;
import org.apache.sqoop.validation.Validation.Message;

/**
 * Test class for org.apache.sqoop.validation.Validation
 */
public class TestValidation extends TestCase {

//  /**
//   * Initialization test
//   */
//  public void testInitialization() {
//    /* Check initialization with class */
//    Validation validation = new Validation(Class.class);
//    assertNotNull(validation);
//    assertEquals(Status.FINE, validation.getStatus());
//    assertEquals(0, validation.getMessages().size());
//
//    /* Check initialization with status and message as null */
//    Validation validationNull = new Validation(null, null);
//    assertNotNull(validationNull);
//    assertNull(validationNull.getStatus());
//    assertNull(validationNull.getMessages());
//
//    /* Check initialization with status and message with values */
//    Status s1 = Status.FINE;
//    Map<FormInput, Message> msg1 = new HashMap<Validation.FormInput, Validation.Message>();
//    Validation validation1 = new Validation(s1, msg1);
//    assertNotNull(validation1);
//    assertEquals(Status.FINE, validation1.getStatus());
//    assertEquals(0, validation1.getMessages().size());
//
//    /* Check initialization with status and message with values */
//    Status s2 = Status.ACCEPTABLE;
//    Map<FormInput, Message> msg2 = new HashMap<Validation.FormInput, Validation.Message>();
//    Validation validation2 = new Validation(s2, msg2);
//    assertNotNull(validation2);
//    assertEquals(Status.ACCEPTABLE, validation2.getStatus());
//    assertEquals(0, validation2.getMessages().size());
//
//    /* Check initialization with status and message with values */
//    Status s3 = Status.ACCEPTABLE;
//    Map<FormInput, Message> msg3 = new HashMap<Validation.FormInput, Validation.Message>();
//    Validation.FormInput fi = new Validation.FormInput("form\\.input");
//    Validation.Message message = new Validation.Message(Status.FINE, "sqoop");
//    msg3.put(fi, message);
//    Validation validation3 = new Validation(s3, msg3);
//    Validation.FormInput fiTest = new Validation.FormInput("form\\.input");
//    Validation.Message messageTest = new Validation.Message(Status.FINE,
//        "sqoop");
//    assertEquals(messageTest, validation3.getMessages().get(fiTest));
//    assertEquals(Status.ACCEPTABLE, validation3.getStatus());
//  }
//
//  /**
//   * Test for Validation.ForInput
//   */
//  public void testFormInput() {
//    Validation.FormInput fi = new Validation.FormInput("test\\.test");
//    assertNotNull(fi);
//
//    /* Passing null */
//    try {
//      new Validation.FormInput(null);
//      fail("Assert error is expected");
//    } catch (AssertionError e) {
//      assertTrue(true);
//    }
//
//    /* Passing empty and check exception messages */
//    try {
//      new Validation.FormInput("");
//      fail("SqoopException is expected");
//    } catch (SqoopException e) {
//      assertEquals(ValidationError.VALIDATION_0003.getMessage(), e
//          .getErrorCode().getMessage());
//    }
//
//    /* Passing value and check */
//    Validation.FormInput fi2 = new Validation.FormInput("form\\.input");
//    assertEquals("form\\", fi2.getForm());
//    assertEquals("input", fi2.getInput());
//
//    /* Check equals */
//    Validation.FormInput fiOne = new Validation.FormInput("form\\.input");
//    Validation.FormInput fiTwo = new Validation.FormInput("form\\.input");
//    assertEquals(fiOne, fiTwo);
//
//    /* toString() method check */
//    assertEquals("form\\.input", fiOne.toString());
//
//    // Checking null as input field (form validation)
//    Validation.FormInput fi3 = new FormInput("form");
//    assertEquals("form", fi3.getForm());
//    assertNull(fi3.getInput());
//    assertEquals("form", fi3.toString());
//
//  }
//
//  /**
//   * Test for Validation.Message
//   */
//  public void testMessage() {
//    /* Passing null */
//    Validation.Message msg1 = new Validation.Message(null, null);
//    assertNull(msg1.getStatus());
//    assertNull(msg1.getMessage());
//
//    /* Passing values */
//    Validation.Message msg2 = new Validation.Message(Status.FINE, "sqoop");
//    assertEquals(Status.FINE, msg2.getStatus());
//    assertEquals("sqoop", msg2.getMessage());
//
//    /* Check for equal */
//    Validation.Message msg3 = new Validation.Message(Status.FINE, "sqoop");
//    assertEquals(msg2, msg3);
//  }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.validation.ConfigValidator.ConfigInput;
import org.apache.sqoop.validation.ConfigValidator.Message;
import org.junit.Test;

/**
 * Test class for org.apache.sqoop.validation.Validation
 */
public class TestValidation {

  /**
   * Initialization test
   */
  @Test
  public void testInitialization() {
    /* Check initialization with class */
    ConfigValidator validation = new ConfigValidator(Class.class);
    assertNotNull(validation);
    assertEquals(Status.FINE, validation.getStatus());
    assertEquals(0, validation.getMessages().size());

    /* Check initialization with status and message as null */
    ConfigValidator validationNull = new ConfigValidator(null, null);
    assertNotNull(validationNull);
    assertNull(validationNull.getStatus());
    assertNull(validationNull.getMessages());

    /* Check initialization with status and message with values */
    Status s1 = Status.FINE;
    Map<ConfigInput, Message> msg1 = new HashMap<ConfigValidator.ConfigInput, ConfigValidator.Message>();
    ConfigValidator validation1 = new ConfigValidator(s1, msg1);
    assertNotNull(validation1);
    assertEquals(Status.FINE, validation1.getStatus());
    assertEquals(0, validation1.getMessages().size());

    /* Check initialization with status and message with values */
    Status s2 = Status.ACCEPTABLE;
    Map<ConfigInput, Message> msg2 = new HashMap<ConfigValidator.ConfigInput, ConfigValidator.Message>();
    ConfigValidator validation2 = new ConfigValidator(s2, msg2);
    assertNotNull(validation2);
    assertEquals(Status.ACCEPTABLE, validation2.getStatus());
    assertEquals(0, validation2.getMessages().size());

    /* Check initialization with status and message with values */
    Status s3 = Status.ACCEPTABLE;
    Map<ConfigInput, Message> msg3 = new HashMap<ConfigValidator.ConfigInput, ConfigValidator.Message>();
    ConfigValidator.ConfigInput fi = new ConfigValidator.ConfigInput("config\\.input");
    ConfigValidator.Message message = new ConfigValidator.Message(Status.FINE, "sqoop");
    msg3.put(fi, message);
    ConfigValidator validation3 = new ConfigValidator(s3, msg3);
    ConfigValidator.ConfigInput fiTest = new ConfigValidator.ConfigInput("config\\.input");
    ConfigValidator.Message messageTest = new ConfigValidator.Message(Status.FINE,
        "sqoop");
    assertEquals(messageTest, validation3.getMessages().get(fiTest));
    assertEquals(Status.ACCEPTABLE, validation3.getStatus());
  }

  /**
   * Test for Validation.ForInput
   */
  public void testConfigInput() {
    ConfigValidator.ConfigInput fi = new ConfigValidator.ConfigInput("test\\.test");
    assertNotNull(fi);

    /* Passing null */
    try {
      new ConfigValidator.ConfigInput(null);
      fail("Assert error is expected");
    } catch (AssertionError e) {
      assertTrue(true);
    }

    /* Passing empty and check exception messages */
    try {
      new ConfigValidator.ConfigInput("");
      fail("SqoopException is expected");
    } catch (SqoopException e) {
      assertEquals(ConfigValidationError.VALIDATION_0003.getMessage(), e
          .getErrorCode().getMessage());
    }

    /* Passing value and check */
    ConfigValidator.ConfigInput fi2 = new ConfigValidator.ConfigInput("config\\.input");
    assertEquals("config\\", fi2.getConfig());
    assertEquals("input", fi2.getInput());

    /* Check equals */
    ConfigValidator.ConfigInput fiOne = new ConfigValidator.ConfigInput("config\\.input");
    ConfigValidator.ConfigInput fiTwo = new ConfigValidator.ConfigInput("config\\.input");
    assertEquals(fiOne, fiTwo);

    /* toString() method check */
    assertEquals("config\\.input", fiOne.toString());

    // Checking null as input field (config validation)
    ConfigValidator.ConfigInput fi3 = new ConfigInput("config");
    assertEquals("config", fi3.getConfig());
    assertNull(fi3.getInput());
    assertEquals("config", fi3.toString());

  }

  /**
   * Test for Validation.Message
   */
  public void testMessage() {
    /* Passing null */
    ConfigValidator.Message msg1 = new ConfigValidator.Message(null, null);
    assertNull(msg1.getStatus());
    assertNull(msg1.getMessage());

    /* Passing values */
    ConfigValidator.Message msg2 = new ConfigValidator.Message(Status.FINE, "sqoop");
    assertEquals(Status.FINE, msg2.getStatus());
    assertEquals("sqoop", msg2.getMessage());

    /* Check for equal */
    ConfigValidator.Message msg3 = new ConfigValidator.Message(Status.FINE, "sqoop");
    assertEquals(msg2, msg3);
  }
}

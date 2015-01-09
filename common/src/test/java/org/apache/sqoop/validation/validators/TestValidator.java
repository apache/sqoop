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
package org.apache.sqoop.validation.validators;

import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 *
 */
public class TestValidator {
  public static class ValidatorImpl extends AbstractValidator<String> {
    @Override
    public void validate(String msg) {
      addMessage(Status.OK, msg);
      addMessage(new Message(Status.ERROR, "Prefix: " + msg));
    }
  }

  @Test
  public void test() {
    ValidatorImpl validator = new ValidatorImpl();

    assertEquals(0, validator.getMessages().size());
    validator.validate("X");
    assertEquals(2, validator.getMessages().size());

    Message msg = validator.getMessages().get(0);
    assertEquals(Status.OK, msg.getStatus());
    assertEquals("X", msg.getMessage());

    msg = validator.getMessages().get(1);
    assertEquals(Status.ERROR, msg.getStatus());
    assertEquals("Prefix: X", msg.getMessage());

    validator.reset();
    assertEquals(0, validator.getMessages().size());
  }
}

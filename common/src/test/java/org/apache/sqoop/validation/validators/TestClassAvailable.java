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

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.sqoop.validation.Message;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.Test;

public class TestClassAvailable {

  AbstractValidator<String> validator = new ClassAvailable();

  @Test
  public void test() {
    List<Message> messages;
    assertEquals(0, validator.getMessages().size());

    validator.validate(ClassAvailable.class.getCanonicalName());
    assertEquals(0, validator.getMessages().size());

    validator.validate("java.lang.String");
    assertEquals(0, validator.getMessages().size());

    validator.validate("net.jarcec.super.private.project.Main");
    assertEquals(1, validator.getMessages().size());
    messages = validator.getMessages();
    assertEquals(Status.ERROR, messages.get(0).getStatus());

    validator.validate(null);
    assertEquals(2, validator.getMessages().size());
    messages = validator.getMessages();
    assertEquals(Status.ERROR, messages.get(1).getStatus());
  }
}

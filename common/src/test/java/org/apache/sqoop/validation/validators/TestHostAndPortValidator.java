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

import org.apache.sqoop.validation.Status;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestHostAndPortValidator {

  AbstractValidator<String> validator = new HostAndPortValidator();

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    validator.reset();
    assertEquals(0, validator.getMessages().size());
  }

  @Test
  public void testValidHostAndPort() {
    expectValid("host1:8020");
  }

  @Test
  public void testValidHost() {
    expectValid("host1");
  }

  private void expectValid(String input) {
    validator.validate(input);
    assertEquals(Status.OK, validator.getStatus());
    assertEquals(0, validator.getMessages().size());
  }

  @Test
  public void testInvalidPort() {
    expectInvalid("host1:invalid_port");
  }

  @Test
  public void testNegativePort() {
    expectInvalid("host1:-1");
  }

  @Test
  public void testHostNameWithInvalidChars() {
    expectInvalid("hostname has space:8020");
  }

  private void expectInvalid(String input) {
    validator.validate(input);
    assertEquals(Status.ERROR, validator.getStatus());
    assertEquals(1, validator.getMessages().size());
  }

}
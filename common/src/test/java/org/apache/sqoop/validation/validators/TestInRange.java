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

import org.apache.sqoop.common.SqoopException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestInRange {

  AbstractValidator validator = new InRange();

  @BeforeMethod
  public void setValidArgumentForValidator() {
    validator.setStringArgument("0,100");
  }

  @AfterMethod
  public void resetValidator() {
    validator.reset();
  }

  @Test
  public void testEmpty() {
    assertEquals(0, validator.getMessages().size());
  }

  @Test
  public void testNoMessagesWhenInRange() {
    validator.validate(0);
    assertEquals(0, validator.getMessages().size());

    validator.validate(100);
    assertEquals(0, validator.getMessages().size());

    validator.validate(50);
    assertEquals(0, validator.getMessages().size());
  }

  @Test
  public void testMessagesWhenOutsideRange() {
    validator.validate(-1);
    assertEquals(1, validator.getMessages().size());

    validator.validate(101);
    assertEquals(2, validator.getMessages().size());
  }

  @Test(expectedExceptions = {SqoopException.class})
  public void testNoArgumentSet() {
    validator.setStringArgument(AbstractValidator.DEFAULT_STRING_ARGUMENT);
    validator.validate(0);
  }

  @Test(expectedExceptions = {SqoopException.class})
  public void testMalformedArgument() {
    validator.setStringArgument("9,hello");
    validator.validate(0);
  }
}

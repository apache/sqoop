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

import org.apache.sqoop.SqoopOptions;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for AbortOnFailureHandler.
 */
public class AbortOnFailureHandlerTest {

  @Test
  public void testAbortOnFailureHandlerIsDefaultOption() {
    assertEquals(AbortOnFailureHandler.class,
      new SqoopOptions(new Configuration()).getValidationFailureHandlerClass());
  }

  /**
   * Positive case.
   */
  @Test
  public void testAbortOnFailureHandlerAborting() {
    try {
      Validator validator = new RowCountValidator();
      validator.validate(new ValidationContext(100, 90));
      fail("AbortOnFailureHandler should have thrown an exception");
    } catch (ValidationException e) {
      assertEquals("Validation failed by RowCountValidator. "
        + "Reason: The expected counter value was 100 but the actual value "
        + "was 90, Row Count at Source: 100, Row Count at Target: 90",
        e.getMessage());
    }
  }

  /**
   * Negative case.
   */
  @Test
  public void testAbortOnFailureHandlerNotAborting() {
    try {
      Validator validator = new RowCountValidator();
      validator.validate(new ValidationContext(100, 100));
    } catch (ValidationException e) {
      fail("AbortOnFailureHandler should NOT have thrown an exception");
    }
  }
}

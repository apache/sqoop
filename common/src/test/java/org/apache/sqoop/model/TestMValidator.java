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

import org.apache.sqoop.validation.validators.AbstractValidator;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

/**
 * Test class for org.apache.sqoop.model.MValidator
 */
public class TestMValidator {
  @Test
  public void testEquality() {
    MValidator testMValidator1 = new MValidator("testValidator", AbstractValidator.DEFAULT_STRING_ARGUMENT);
    MValidator testMValidator2 = new MValidator("testValidator", AbstractValidator.DEFAULT_STRING_ARGUMENT);
    assertEquals(testMValidator1, testMValidator2);
  }
}

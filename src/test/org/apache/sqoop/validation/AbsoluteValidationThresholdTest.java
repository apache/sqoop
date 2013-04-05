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

import junit.framework.TestCase;

/**
 * Tests for AbsoluteValidationThreshold.
 */
public class AbsoluteValidationThresholdTest extends TestCase {

  /**
   * Test the implementation for AbsoluteValidationThreshold.
   * Both arguments should be same else fail.
   */
  public void testAbsoluteValidationThreshold() {
    ValidationThreshold validationThreshold = new AbsoluteValidationThreshold();
    assertTrue(validationThreshold.compare(100, 100));
    assertFalse(validationThreshold.compare(100, 90));
    assertFalse(validationThreshold.compare(90, 100));
  }
}

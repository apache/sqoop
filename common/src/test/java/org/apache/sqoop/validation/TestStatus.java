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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 *
 */
public class TestStatus {
  @Test
  public void testGetWorstStatus() {
    // Comparing itself with itself
    assertEquals(Status.OK,
      Status.getWorstStatus(Status.OK));
    assertEquals(Status.OK,
      Status.getWorstStatus(Status.OK, Status.OK));
    assertEquals(Status.WARNING,
      Status.getWorstStatus(Status.WARNING, Status.WARNING));
    assertEquals(Status.ERROR,
      Status.getWorstStatus(Status.ERROR, Status.ERROR));

    // Retriving the worst option
    assertEquals(Status.ERROR,
      Status.getWorstStatus(Status.OK, Status.ERROR));
    assertEquals(Status.WARNING,
      Status.getWorstStatus(Status.OK, Status.WARNING));
  }

  @Test
  public void testCanProceed() {
    assertTrue(Status.OK.canProceed());
    assertTrue(Status.WARNING.canProceed());
    assertFalse(Status.ERROR.canProceed());
  }
}

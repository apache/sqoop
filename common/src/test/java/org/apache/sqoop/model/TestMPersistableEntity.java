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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TestMPersistableEntity {

  @Test
  public void testPersistableId() {
    PersistentId id = new PersistentId();

    assertFalse(id.hasPersistenceId());

    id.setPersistenceId(666);
    assertTrue(id.hasPersistenceId());
    assertEquals(666, id.getPersistenceId());
  }

  /**
   * Testing class extending MPersistableEntity.
   *
   * Empty implementation with purpose to just test methods available
   * directly in the abstract class.
   */
  public static class PersistentId extends MPersistableEntity {
    @Override
    public String toString() {
      return null;
    }
  }

}

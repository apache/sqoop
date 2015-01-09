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
package org.apache.sqoop.schema.type;

import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.testng.annotations.Test;

public class TestEnum {

  @Test
  public void testEnumWithNoOptions() {
    Enum e1 = new Enum("A");
    Enum e2 = new Enum("A");
    assertTrue(e1.equals(e2));
    assertEquals(e1.toString(), e2.toString());
  }

  @Test
  public void testEnumWithDifferentOptions() {
    Enum e1 = new Enum("A").setOptions(Collections.unmodifiableSet(new HashSet<String>(Arrays
        .asList(new String[] { "A", "B" }))));
    Enum e2 = new Enum("A").setOptions(Collections.unmodifiableSet(new HashSet<String>(Arrays
        .asList(new String[] { "A1", "B1" }))));
    assertFalse(e1.equals(e2));
    assertNotEquals(e1.toString(), e2.toString());
  }

  @Test
  public void testEnumWithSameOptions() {
    Enum e1 = new Enum("A").setOptions(Collections.unmodifiableSet(new HashSet<String>(Arrays
        .asList(new String[] { "A", "B" }))));
    Enum e2 = new Enum("A").setOptions(Collections.unmodifiableSet(new HashSet<String>(Arrays
        .asList(new String[] { "A", "B" }))));
    assertTrue(e1.equals(e2));
    assertEquals(e1.toString(), e2.toString());
  }

}

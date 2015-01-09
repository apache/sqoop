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

import org.testng.annotations.Test;

public class TestMap {

  @Test
  public void testMapWithSameListKeyValue() {
    Map m1 = new Map("m1", new Text("T"), new Text("T"));
    Map m2 = new Map("m1", new Text("T"), new Text("T"));
    assertTrue(m1.equals(m2));
    assertEquals(m1.toString(), m2.toString());
  }

  @Test
  public void testMapWithDifferentName() {
    Map m1 = new Map("m1", new Text("T"), new Text("T"));
    Map m2 = new Map("m2", new Text("T"), new Text("T"));
    assertFalse(m1.equals(m2));
    assertNotEquals(m1.toString(), m2.toString());
  }

  @Test
  public void testMapWithDifferentKey() {
    Map m1 = new Map("m1", new Text("T"), new Text("T"));
    Map m2 = new Map("m1", new Text("T2"), new Text("T"));
    assertFalse(m1.equals(m2));
    assertNotEquals(m1.toString(), m2.toString());
  }

  @Test
  public void testMapWithDifferentValue() {
    Map m1 = new Map("m1", new Text("T"), new Text("T2"));
    Map m2 = new Map("m1", new Text("T2"), new Text("T4"));
    assertFalse(m1.equals(m2));
    assertNotEquals(m1.toString(), m2.toString());
  }
}

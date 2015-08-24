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
package org.apache.sqoop.common;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestMutableMapContext {

  MutableMapContext context;

  @BeforeTest
  public void setUpContext() {
    context = new MutableMapContext();
  }

  @Test
  public void testConstructors() {
    assertNull(context.getString("random.key"));

    Map<String, String> map = new HashMap<>();
    map.put("string", "A");
    map.put("long", "1");
    map.put("integer", "13");
    map.put("boolean", "true");

    context = new MutableMapContext(map);
    assertEquals(context.getString("string"), "A");
    assertEquals(context.getLong("long", -1), 1L);
    assertEquals(context.getInt("integer", -1), 13);
    assertEquals(context.getBoolean("boolean", false), true);
  }

  @Test
  public void testSetString() {
    context.setString("a", "b");
    assertEquals(context.getString("a"), "b");
  }

  @Test
  public void testSetLong() {
    context.setLong("a", 1L);
    assertEquals(context.getLong("a", -1L), 1L);
  }

  @Test
  public void testSetInteger() {
    context.setInteger("a", 1);
    assertEquals(context.getInt("a", -1), 1);
  }

  @Test
  public void testSetBoolean() {
    context.setBoolean("a", true);
    assertEquals(context.getBoolean("a", false), true);
  }

  @Test
  public void testSetAll() {
    // Pre-setting few properties
    context.setString("string", "original_value");
    context.setLong("long", 55L);

    // Generate map that contains few properties that are already in the context and some new properties
    Map<String, String> map = new HashMap<>();
    map.put("string", "A");
    map.put("long", "1");
    map.put("integer", "13");
    map.put("boolean", "true");

    // Calling setAll should set all new properties and override the existing ones
    context.setAll(map);
    assertEquals(context.getString("string"), "A");
    assertEquals(context.getLong("long", -1), 1L);
    assertEquals(context.getInt("integer", -1), 13);
    assertEquals(context.getBoolean("boolean", false), true);

    // Verify that we're resilient against null
    context.setAll(null);
  }

}

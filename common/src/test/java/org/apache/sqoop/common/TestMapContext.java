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

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test class for org.apache.sqoop.common.MapContext
 */
public class TestMapContext {

  /**
   * Test method for Initialization
   */
  @Test
  public void testInitalization() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("testkey", "testvalue");
    MapContext mc = new MapContext(options);
    Assert.assertEquals("testvalue", mc.getString("testkey"));
  }

  /**
   * Test method for getString
   */
  @Test
  public void testGetString() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("testkey", "testvalue");
    MapContext mc = new MapContext(options);
    Assert.assertEquals("testvalue", mc.getString("testkey", "defaultValue"));
    Assert.assertEquals("defaultValue",
        mc.getString("wrongKey", "defaultValue"));
  }

  /**
   * Test method for getString with default value
   */
  @Test
  public void testGetBoolean() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("testkey", "true");
    MapContext mc = new MapContext(options);
    Assert.assertEquals(true, mc.getBoolean("testkey", false));
    Assert.assertEquals(false, mc.getBoolean("wrongKey", false));
  }

  /**
   * Test method for getInt with default value
   */
  @Test
  public void testGetInt() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("testkey", "123");
    MapContext mc = new MapContext(options);
    Assert.assertEquals(123, mc.getInt("testkey", 456));
    Assert.assertEquals(456, mc.getInt("wrongKey", 456));
  }

  /**
   * Test method for getLong with default value
   */
  @Test
  public void testGetLong() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("testkey", "123");
    MapContext mc = new MapContext(options);
    Assert.assertEquals(123l, mc.getLong("testkey", 456l));
    Assert.assertEquals(456l, mc.getLong("wrongKey", 456l));
  }

  /**
   * Test method for getNestedProperties()
   */
  @Test
  public void testGetNestedProperties() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("sqooptest1", "value");
    options.put("sqooptest2", "value");
    options.put("testsqoop1", "value");
    options.put("testsqoop1", "value");
    MapContext mc = new MapContext(options);
    Map<String, String> result = mc.getNestedProperties("sqoop");
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.containsKey("test1"));
    Assert.assertTrue(result.containsKey("test2"));
    Assert.assertFalse(result.containsKey("testsqoop1"));
    Assert.assertFalse(result.containsKey("testsqoop2"));
  }

  /**
   * Test iteration
   */
  @Test
  public void testIterator() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("sqooptest1", "value");
    options.put("sqooptest2", "value");

    MapContext mc = new MapContext(options);
    boolean seenSqooptest1 = false;
    boolean seenSqooptest2 = false;
    for(Map.Entry<String, String> entry : mc) {
      if("sqooptest1".equals(entry.getKey()) && "value".equals(entry.getValue())) {
        seenSqooptest1 = true;
      } else if("sqooptest2".equals(entry.getKey()) && "value".equals(entry.getValue())) {
        seenSqooptest2 = true;
      } else {
        fail("Found unexpected property: " + entry.getKey() + " with value " + entry.getValue());
      }
    }

    assertTrue(seenSqooptest1);
    assertTrue(seenSqooptest2);
  }
}

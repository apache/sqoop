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
package org.apache.sqoop.job;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrefixContext {

  @Test
  public void testBlankPrefix() {
    Configuration configuration = new Configuration();
    configuration.set("testkey", "testvalue");

    PrefixContext context = new PrefixContext(configuration, "");
    assertEquals("testvalue", context.getString("testkey"));
  }

  @Test
  public void testNonBlankPrefix() {
    Configuration configuration = new Configuration();
    configuration.set("prefix.testkey", "testvalue");

    PrefixContext context = new PrefixContext(configuration, "prefix.");
    assertEquals("testvalue", context.getString("testkey"));
  }

  @Test
  public void testGetString() {
    Configuration configuration = new Configuration();
    configuration.set("p.testkey", "testvalue");

    PrefixContext context = new PrefixContext(configuration, "p.");
    assertEquals("testvalue", context.getString("testkey"));
    assertEquals("testvalue", context.getString("testkey", "defaultValue"));
    assertEquals("defaultValue", context.getString("wrongKey", "defaultValue"));
  }

  @Test
  public void testGetBoolean() {
    Configuration configuration = new Configuration();
    configuration.set("p.testkey", "true");

    PrefixContext context = new PrefixContext(configuration, "p.");
    assertEquals(true, context.getBoolean("testkey", false));
    assertEquals(false, context.getBoolean("wrongKey", false));
  }

  @Test
  public void testGetInt() {
    Configuration configuration = new Configuration();
    configuration.set("p.testkey", "123");

    PrefixContext context = new PrefixContext(configuration, "p.");
    assertEquals(123, context.getInt("testkey", 456));
    assertEquals(456, context.getInt("wrongKey", 456));
  }

  @Test
  public void testGetLong() {
    Configuration configuration = new Configuration();
    configuration.set("p.testkey", "123");

    PrefixContext context = new PrefixContext(configuration, "p.");
    assertEquals(123l, context.getLong("testkey", 456l));
    assertEquals(456l, context.getLong("wrongKey", 456l));
  }

  @Test
  public void testIterator() {
    Configuration configuration = new Configuration();
    configuration.set("p.sqooptest1", "value");
    configuration.set("p.sqooptest2", "value");

    PrefixContext context = new PrefixContext(configuration, "p.");
    boolean seenSqooptest1 = false;
    boolean seenSqooptest2 = false;
    for(Map.Entry<String, String> entry : context) {
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

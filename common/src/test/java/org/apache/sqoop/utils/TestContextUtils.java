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
package org.apache.sqoop.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.sqoop.common.MapContext;
import org.testng.annotations.Test;

import static org.apache.sqoop.utils.ContextUtils.getArrayOfStrings;
import static org.apache.sqoop.utils.ContextUtils.getUniqueStrings;
import static org.testng.Assert.assertEquals;

/**
 * Test class for org.apache.sqoop.utils.ContextUtils
 */
public class TestContextUtils {
  @Test
  public void testGetArrayOfStrings() {
    final String DEFAULT_SEPARATOR = ",";

    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "value1:value2");
    options1.put("sqooptest2", "value1");

    Map<String, String> options2 = new HashMap<String, String>();
    options2.put("sqooptest1", "value1,value2");
    options2.put("sqooptest2", "value1");

    MapContext mc1 = new MapContext(options1);
    MapContext mc2 = new MapContext(options2);

    assertEquals(getArrayOfStrings(mc1.getString("sqooptest1")), new String[]{"value1", "value2"});
    assertEquals(getArrayOfStrings(mc1.getString("sqooptest2")), new String[]{"value1"});

    assertEquals(getArrayOfStrings(mc2.getString("sqooptest1"), DEFAULT_SEPARATOR), new String[]{"value1", "value2"});
    assertEquals(getArrayOfStrings(mc2.getString("sqooptest2"), DEFAULT_SEPARATOR), new String[]{"value1"});
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetArrayOfStringsNullValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", null);
    MapContext mc1 = new MapContext(options1);
    getArrayOfStrings(mc1.getString("sqooptest1"));
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetArrayOfStringsWithSeparatorNullValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", null);
    MapContext mc1 = new MapContext(options1);
    getArrayOfStrings(mc1.getString("sqooptest1"), ",");
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetArrayOfStringsEmptyValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "");
    MapContext mc1 = new MapContext(options1);
    getArrayOfStrings(mc1.getString("sqooptest1"));
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetArrayOfStringsWithSeparatorEmptyValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "");
    MapContext mc1 = new MapContext(options1);
    getArrayOfStrings(mc1.getString("sqooptest1"), ",");
  }

  @Test
  public void testGetUniqueStrings() {
    final String DEFAULT_SEPARATOR = ",";

    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "value1:value2");
    options1.put("sqooptest2", "value1");
    options1.put("sqooptest3", "value1:value2:value1");

    Map<String, String> options2 = new HashMap<String, String>();
    options2.put("sqooptest1", "value1,value2");
    options2.put("sqooptest2", "value1");
    options2.put("sqooptest3", "value1,value2,value1");

    MapContext mc1 = new MapContext(options1);
    MapContext mc2 = new MapContext(options2);

    assertEquals(getUniqueStrings(mc1.getString("sqooptest1")), new HashSet<String>(Arrays.asList(new String[]{"value1", "value2"})));
    assertEquals(getUniqueStrings(mc1.getString("sqooptest2")), new HashSet<String>(Arrays.asList(new String[]{"value1"})));
    assertEquals(getUniqueStrings(mc1.getString("sqooptest3")), new HashSet<String>(Arrays.asList(new String[]{"value1", "value2"})));

    assertEquals(getUniqueStrings(mc2.getString("sqooptest1"), DEFAULT_SEPARATOR), new HashSet<String>(Arrays.asList(new String[]{"value1", "value2"})));
    assertEquals(getUniqueStrings(mc2.getString("sqooptest2"), DEFAULT_SEPARATOR), new HashSet<String>(Arrays.asList(new String[]{"value1"})));
    assertEquals(getUniqueStrings(mc2.getString("sqooptest3"), DEFAULT_SEPARATOR), new HashSet<String>(Arrays.asList(new String[]{"value1", "value2"})));
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetUniqueStringsNullValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", null);
    MapContext mc1 = new MapContext(options1);
    getUniqueStrings(mc1.getString("sqooptest1"));
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetUniqueStringsWithSeparatorNullValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", null);
    MapContext mc1 = new MapContext(options1);
    getUniqueStrings(mc1.getString("sqooptest1"), ",");
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetUniqueStringsEmptyValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "");
    MapContext mc1 = new MapContext(options1);
    getUniqueStrings(mc1.getString("sqooptest1"));
  }

  @Test(expectedExceptions = {AssertionError.class})
  public void testGetUniqueStringsWithSeparatorEmptyValue() {
    Map<String, String> options1 = new HashMap<String, String>();
    options1.put("sqooptest1", "");
    MapContext mc1 = new MapContext(options1);
    getUniqueStrings(mc1.getString("sqooptest1"), ",");
  }
}

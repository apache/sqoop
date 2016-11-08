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

package org.apache.sqoop.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSqoopJsonUtil {

  private static Map<String, String> paramMap;
  private static String jsonStr;

  @BeforeClass
  public static void setup() {
    paramMap = new HashMap<String, String>();
    paramMap.put("k1", "v1");
    paramMap.put("k2", "v2");
    paramMap.put("k3", "v3");

    jsonStr = "{\"k3\":\"v3\",\"k1\":\"v1\",\"k2\":\"v2\"}";

  }

  @Test
  public void testMapToStringAndBack() {
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(paramMap);
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString(jsonStr);
    assertEquals(paramMap, resultMap);
  }

  @Test
  public void testGetJsonStringFromMapNullMap() {
    Map<String, String> nullMap = null;
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(nullMap);
    assertEquals("{}", resultJsonStr);
  }

  @Test
  public void testGetJsonStringFromMapEmptyMap() {
    Map<String, String> nullMap = new HashMap<String, String>();
    String resultJsonStr = SqoopJsonUtil.getJsonStringforMap(nullMap);
    assertEquals("{}", resultJsonStr);
  }

  @Test
  public void testGetMapforJsonString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString(jsonStr);
    assertEquals(paramMap, resultMap);
  }

  @Test
  public void testGetMapforJsonStringNullString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString(null);
    assertTrue(resultMap.isEmpty());
  }

  @Test
  public void testGetMapforJsonStringEmptyString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString("");
    assertTrue(resultMap.isEmpty());
  }

  @Test
  public void testGetMapforJsonStringEmptyMapString() {
    Map<String, String> resultMap = SqoopJsonUtil.getMapforJsonString("{}");
    assertTrue(resultMap.isEmpty());
  }

  @Test
  public void testEmptyJSON() {
    String jsonStr = null;
    boolean isEmpty;
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

    jsonStr = "";
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

    jsonStr = "{}";
    isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(true, isEmpty);

  }

  @Test
  public void testNonEmptyJSON() {
    boolean isEmpty = SqoopJsonUtil.isEmptyJSON(jsonStr);
    assertEquals(false, isEmpty);
  }

}

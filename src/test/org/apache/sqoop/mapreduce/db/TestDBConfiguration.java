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

package org.apache.sqoop.mapreduce.db;

import java.util.Properties;

import junit.framework.TestCase;

/**
 * Test aspects of DBConfiguration.
 */
public class TestDBConfiguration extends TestCase {

  public void testPropertiesToString() {
    Properties connParams = new Properties();
    connParams.setProperty("a", "value-a");
    connParams.setProperty("b", "value-b");
    connParams.setProperty("a.b", "value-a.b");
    connParams.setProperty("a.b.c", "value-a.b.c");
    connParams.setProperty("aaaaaaaaaa.bbbbbbb.cccccccc", "value-abc");
    String result = DBConfiguration.propertiesToString(connParams);
    Properties resultParams = DBConfiguration.propertiesFromString(result);
    assertEquals("connection params don't match", connParams, resultParams);

    connParams = new Properties();
    connParams.put("conn.timeout", "3000");
    connParams.put("conn.buffer_size", "256");
    connParams.put("conn.dummy", "dummy");
    connParams.put("conn.foo", "bar");
    result = DBConfiguration.propertiesToString(connParams);
    resultParams = DBConfiguration.propertiesFromString(result);
    assertEquals("connection params don't match", connParams, resultParams);

    connParams = new Properties();
    connParams.put("user", "ABC");
    connParams.put("password", "complex\"pass,word\\123");
    connParams.put("complex\"param,\\name", "dummy");
    connParams.put("conn.buffer=size", "256");
    connParams.put("jdbc.property", "a=b");
    result = DBConfiguration.propertiesToString(connParams);
    resultParams = DBConfiguration.propertiesFromString(result);
    assertEquals("connection params don't match", connParams, resultParams);
  }

}

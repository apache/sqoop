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
package org.apache.sqoop.connector.kite.configuration;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test configuration objects.
 */
public class TestConfigUtil {

  @Test
  public void testBuildDatasetUri() {
    String actual = ConfigUtil.buildDatasetUri("namenode:8020",
        "dataset:hdfs:/path/to/ds");
    assertEquals("dataset:hdfs://namenode:8020/path/to/ds", actual);
  }

  @Test
  public void testBuildDatasetUriHdfsHostPortIgnored() {
    String expected = "dataset:hdfs://namenode2:8020/path/to/ds";
    String actual = ConfigUtil.buildDatasetUri("namenode:8020", expected);
    assertEquals(expected, actual);
  }

}
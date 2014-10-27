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
package org.apache.sqoop.connector.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestHdfsUtils {

  @Test
  public void testConfigureURI() throws Exception {
    final String TEST_URI = "hdfs://argggg:1111";
    LinkConfiguration linkConfiguration = new LinkConfiguration();
    Configuration conf = new Configuration();

    assertNotEquals(TEST_URI, conf.get("fs.default.name"));
    assertNotEquals(TEST_URI, conf.get("fs.defaultFS"));

    linkConfiguration.linkConfig.uri = TEST_URI;

    assertEquals(conf, HdfsUtils.configureURI(conf, linkConfiguration));
    assertEquals(TEST_URI, conf.get("fs.default.name"));
    assertEquals(TEST_URI, conf.get("fs.defaultFS"));
  }
}
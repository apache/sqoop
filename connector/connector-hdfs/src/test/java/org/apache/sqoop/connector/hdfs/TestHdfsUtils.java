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
import org.apache.sqoop.connector.hdfs.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.connector.hdfs.configuration.ToJobConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestHdfsUtils {

  @Test
  public void testConfigureURI() throws Exception {
    final String TEST_URI = "hdfs://argggg:1111";
    LinkConfiguration linkConfiguration = new LinkConfiguration();
    Configuration conf = new Configuration();

    assertNotEquals(TEST_URI, conf.get("fs.default.name"));
    assertNotEquals(TEST_URI, conf.get("fs.defaultFS"));

    linkConfiguration.linkConfig.uri = TEST_URI;

    conf = HdfsUtils.configureURI(conf, linkConfiguration);
    assertEquals(TEST_URI, conf.get("fs.default.name"));
    assertEquals(TEST_URI, conf.get("fs.defaultFS"));
  }

  @Test
   public void testIsModifiable() throws Exception {
    LinkConfiguration linkConfiguration = new LinkConfiguration();
    FromJobConfiguration fromJobConfiguration = new FromJobConfiguration();
    ToJobConfiguration toJobConfiguration = new ToJobConfiguration();

    // No configuration
    assertFalse(HdfsUtils.hasCustomFormat(linkConfiguration, fromJobConfiguration));
    assertFalse(HdfsUtils.hasCustomFormat(linkConfiguration, toJobConfiguration));

    // Without override
    fromJobConfiguration.fromJobConfig.nullValue = "\0";
    toJobConfiguration.toJobConfig.nullValue = "\0";
    assertFalse(HdfsUtils.hasCustomFormat(linkConfiguration, fromJobConfiguration));
    assertFalse(HdfsUtils.hasCustomFormat(linkConfiguration, toJobConfiguration));

    // With override
    fromJobConfiguration.fromJobConfig.overrideNullValue = true;
    toJobConfiguration.toJobConfig.overrideNullValue = true;
    assertTrue(HdfsUtils.hasCustomFormat(linkConfiguration, fromJobConfiguration));
    assertTrue(HdfsUtils.hasCustomFormat(linkConfiguration, toJobConfiguration));
  }

  @Test
  public void testTransformRecord() throws Exception {
    LinkConfiguration linkConfiguration = new LinkConfiguration();
    FromJobConfiguration fromJobConfiguration = new FromJobConfiguration();
    ToJobConfiguration toJobConfiguration = new ToJobConfiguration();
    final Object[] fromRecord = new Object[]{
        "'Abe'",
        "\0",
        "'test'"
    };
    final Object[] toRecord = new Object[]{
      "'Abe'",
      "\0",
      "'test'"
    };

    // No transformations
    assertArrayEquals(toRecord, HdfsUtils.formatRecord(linkConfiguration, fromJobConfiguration, fromRecord));
    assertArrayEquals(fromRecord, HdfsUtils.formatRecord(linkConfiguration, toJobConfiguration, toRecord));
  }
}
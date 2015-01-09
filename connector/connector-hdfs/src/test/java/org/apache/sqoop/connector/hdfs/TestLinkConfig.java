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

import org.apache.sqoop.connector.hdfs.configuration.LinkConfig;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TestLinkConfig {
  @Test
  public void testValidURI() {
    String[] URIs = {
        "hdfs://localhost:8020",
        "hdfs://localhost:8020/",
        "hdfs://localhost:8020/test",
        "hdfs://localhost:8020/test/test",
        "hdfs://localhost:8020/test/",
        "hdfs://localhost/",
        "hdfs://localhost",
        "hdfs://a:8020",
        "file:///",
        "file://localhost/",
        "file://localhost/tmp",
        "file://localhost/tmp/"
    };
    for (String uri : URIs) {
      LinkConfig config = new LinkConfig();
      LinkConfig.ConfigValidator validator = new LinkConfig.ConfigValidator();
      config.uri = uri;
      validator.validate(config);
      assertTrue(uri, validator.getStatus().canProceed());
    }
  }

  @Test
  public void testInvalidURI() {
    String[] URIs = {
        "://localhost:8020",
        ":///",
        "://",
        "hdfs:",
        "hdfs//",
        "file//localhost/",
        "-://localhost/"
    };
    for (String uri : URIs) {
      LinkConfig config = new LinkConfig();
      LinkConfig.ConfigValidator validator = new LinkConfig.ConfigValidator();
      config.uri = uri;
      validator.validate(config);
      assertFalse(uri, validator.getStatus().canProceed());
    }
  }
}

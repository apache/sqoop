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
import org.apache.sqoop.connector.hdfs.configuration.LinkConfiguration;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MMapInput;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
      assertTrue(validator.getStatus().canProceed(), uri);
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
      assertFalse(validator.getStatus().canProceed(), uri);
    }
  }

  @Test
  public void testSensitiveConfigOverridesKeys() {
    String nonSensitiveKey = "public";
    String valueString = "value";
    String filler = "blah";

    String[] sensitiveWords = new String[] {"password", "key", "secret"};

    LinkConfiguration linkConfiguration = new LinkConfiguration();
    LinkConfig config = new LinkConfig();
    linkConfiguration.linkConfig = config;
    config.configOverrides.put(nonSensitiveKey, valueString);
    for (String sensitiveWord : sensitiveWords) {
      for (String sensitiveWordWithCase : new String[] {sensitiveWord, sensitiveWord.toUpperCase()}) {
        config.configOverrides.put(filler + sensitiveWordWithCase + filler, valueString);
        config.configOverrides.put(sensitiveWordWithCase + filler, valueString);
        config.configOverrides.put(filler + sensitiveWordWithCase, valueString);
      }
    }

    MConfig mLinkConfig = ConfigUtils.toConfigs(linkConfiguration).get(0);
    MMapInput mConfigOverrides = mLinkConfig.getMapInput("linkConfig.configOverrides");

    Map<String, String> redactedMap = mConfigOverrides.getNonsenstiveValue();
    assertEquals(redactedMap.get(nonSensitiveKey), valueString);

    for (String sensitiveWord : sensitiveWords) {
      for (String sensitiveWordWithCase : new String[] {sensitiveWord, sensitiveWord.toUpperCase()}) {
        assertEquals(redactedMap.get(filler + sensitiveWordWithCase + filler), MMapInput.SENSITIVE_VALUE_PLACEHOLDER);
        assertEquals(redactedMap.get(sensitiveWordWithCase + filler), MMapInput.SENSITIVE_VALUE_PLACEHOLDER);
        assertEquals(redactedMap.get(filler + sensitiveWordWithCase), MMapInput.SENSITIVE_VALUE_PLACEHOLDER);
      }
    }
  }
}

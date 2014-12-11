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
package org.apache.sqoop.connector.kafka;

import org.apache.sqoop.connector.kafka.configuration.LinkConfig;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestConfigValidator {
  @Test
  public void testValidURI() {
    String[] URI = {
            "broker1:9092",
            "broker1:9092,broker2:9092",
            "zk1:2181/kafka",
            "zk1:2181,zk2:2181/kafka"
    };

    for (String uri: URI) {
      LinkConfig.CSVURIValidator validator = new LinkConfig.CSVURIValidator();
      validator.validate(uri);
      assertTrue(validator.getStatus().canProceed());
    }
  }

  @Test
  public void testInvalidURI() {
    String[] URI = {
            "",
            "broker",
            "broker1:9092,broker"
    };
    for (String uri: URI) {
      LinkConfig.CSVURIValidator validator = new LinkConfig.CSVURIValidator();
      validator.validate(uri);
      assertFalse(validator.getStatus().canProceed());
    }


  }

}

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
package org.apache.sqoop.integration.connector.kafka;

import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.KafkaConnectorTestCase;
import org.testng.annotations.Test;


public class FromHDFSToKafkaTest extends KafkaConnectorTestCase {

  public static final String[] input = {
          "A BIRD came down the walk:",
          "He did not know I saw;",
          "He bit an angle-worm in halves",
          "And ate the fellow raw."
  };
  @Test
  public void testBasic() throws Exception {
    createFromFile("input-0001",input);

    // Create Kafka link
    MLink kafkaLink = getClient().createLink("kafka-connector");
    fillKafkaLinkConfig(kafkaLink);
    saveLink(kafkaLink);

    // HDFS link
    MLink hdfsLink = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsLink);
    saveLink(hdfsLink);

    // Job creation
    MJob job = getClient().createJob(hdfsLink.getPersistenceId(), kafkaLink.getPersistenceId());

    // Job connector configs
    fillHdfsFromConfig(job);
    fillKafkaToConfig(job);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    saveJob(job);

    executeJob(job);

    // this will assert the content of the array matches the content of the topic
    validateContent(input);
  }


}
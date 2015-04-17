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
package org.apache.sqoop.common.test.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class encapsulates a real cluster Kafka service and enables tests
 * to run kafka tasks against real cluster
 */

public class KafkaRealRunner extends KafkaRunnerBase {

  private static final Logger logger = LoggerFactory.getLogger(KafkaLocalRunner.class);
  private String kafkaServerUrl;
  private String zkConnectionString;
  private final String KAFKA_SERVER_URL_PROPERTY = "sqoop.kafka.server.url";
  private final String ZK_CONNECTION_STRING_PROPERTY = "sqoop.kafka.zookeeper.url";

  public KafkaRealRunner() {
    logger.info("Setting up kafka to point to real cluster");
    kafkaServerUrl = System.getProperty(KAFKA_SERVER_URL_PROPERTY);
    if(kafkaServerUrl == null) {
      logger.error("To run against real cluster, sqoop.kafka.server.url must be provided");
      throw new RuntimeException("To run against real cluster, sqoop.kafka.server.url must be provided");
    }
    logger.info("Kafka server url: " + kafkaServerUrl);

    zkConnectionString = System.getProperty(
        ZK_CONNECTION_STRING_PROPERTY);
    if(zkConnectionString == null) {
      logger.error("To run against real cluster, sqoop.kafka.zookeeper.url must be provided");
      throw new RuntimeException("To run against real cluster, sqoop.kafka.zookeeper.url must be provided");
    }
    logger.info("Zookeeper server connection string: " + zkConnectionString);
  }
  @Override
  public void start() throws Exception {
    // nothing to be done for real server
  }

  @Override
  public void stop() throws IOException {
    // nothing to be done for real server
  }

  @Override
  public String getZkConnectionString() {
    return this.zkConnectionString;
  }

  @Override
  public String getKafkaUrl() {
    return this.kafkaServerUrl;
  }
}

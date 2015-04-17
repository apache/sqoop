/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.sqoop.common.test.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * A local Kafka server for running unit tests.
 * Reference: https://gist.github.com/fjavieralba/7930018/
 */
public class KafkaLocalRunner extends KafkaRunnerBase {

  public KafkaServerStartable kafka;
  public ZooKeeperLocal zookeeperServer;
  private KafkaConfig kafkaConfig;
  private int kafkaLocalPort = 9022;
  private int zkLocalPort = 2188;
  private static final Logger logger = LoggerFactory.getLogger(KafkaLocalRunner.class);

  public KafkaLocalRunner() throws IOException,
          InterruptedException{
    kafkaLocalPort = NetworkUtils.findAvailablePort();
    zkLocalPort = NetworkUtils.findAvailablePort();

    logger.info("Starting kafka server with kafka port " + kafkaLocalPort +
        " and zookeeper port " + zkLocalPort );
    try {
      //start local Zookeeper
      zookeeperServer = new ZooKeeperLocal(zkLocalPort);
      logger.info("ZooKeeper instance is successfully started on port " +
          zkLocalPort);

      Properties kafkaProperties = getKafkaProperties();
      kafkaConfig = new KafkaConfig(kafkaProperties);

      //start local kafka broker
      kafka = new KafkaServerStartable(kafkaConfig);
      logger.info("Kafka Server is successfully started on port " +
          kafkaLocalPort);

    } catch (Exception e) {
      logger.error("Error starting the Kafka Server.", e);
    }

  }

  Properties getKafkaProperties() {
    Properties kafkaProps = new Properties();
    kafkaProps.put("broker.id","0");
    // Kafka expects strings for all properties and KafkaConfig will throw an exception otherwise
    kafkaProps.put("port",Integer.toString(kafkaLocalPort));
    kafkaProps.put("log.dirs","target/kafka-logs");
    kafkaProps.put("num.partitions","1");
    kafkaProps.put("zookeeper.connect",zookeeperServer.getConnectString());

    return kafkaProps;
  }

  @Override
  public void start() throws Exception {
    kafka.startup();
  }

  @Override
  public void stop() throws IOException {
    kafka.shutdown();
    zookeeperServer.stopZookeeper();
    File dir = new File(kafkaConfig.logDirs().head()).getAbsoluteFile();
    FileUtils.deleteDirectory(dir);
  }

  @Override
  public String getZkConnectionString() {
    return zookeeperServer.getConnectString();
  }

  @Override
  public String getKafkaUrl() {
    return "localhost:"+kafkaLocalPort;
  }

}
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

import kafka.message.MessageAndMetadata;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * A utility class for starting/stopping Kafka Server.
 */
public class TestUtil {

  private static final Logger logger = LoggerFactory.getLogger(TestUtil.class);
  private static TestUtil instance = new TestUtil();

  private Random randPortGen = new Random(System.currentTimeMillis());
  private KafkaLocal kafkaServer;
  private ZooKeeperLocal zookeeperServer;
  private KafkaConsumer kafkaConsumer;
  private String hostname = "localhost";
  private int kafkaLocalPort = 9022;
  private int zkLocalPort = 2188;

  private TestUtil() {
    init();
  }

  public static TestUtil getInstance() {
    return instance;
  }

  private void init() {
    // get the localhost.
    try {
      hostname = InetAddress.getLocalHost().getHostName();

    } catch (UnknownHostException e) {
      logger.warn("Error getting the value of localhost. " +
              "Proceeding with 'localhost'.", e);
    }
  }

  private boolean startKafkaServer() throws IOException {
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

      kafkaServer = new KafkaLocal(kafkaProperties);
      kafkaServer.start();

      logger.info("Kafka Server is successfully started on port " +
              kafkaLocalPort);
      return true;

    } catch (Exception e) {
      logger.error("Error starting the Kafka Server.", e);
      return false;
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

  private KafkaConsumer getKafkaConsumer() {
    synchronized (this) {
      if (kafkaConsumer == null) {
        kafkaConsumer = new KafkaConsumer();
      }
    }
    return kafkaConsumer;
  }

  public void initTopicList(List<String> topics) {
    getKafkaConsumer().initTopicList(topics);
  }

  public MessageAndMetadata getNextMessageFromConsumer(String topic) {
    return getKafkaConsumer().getNextMessage(topic);
  }

  public void prepare() throws IOException {
    boolean startStatus = startKafkaServer();
    if (!startStatus) {
      throw new RuntimeException("Error starting the server!");
    }
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    getKafkaConsumer();
    logger.info("Completed the prepare phase.");
  }

  public void tearDown() throws IOException {
    logger.info("Shutting down the Kafka Consumer.");
    getKafkaConsumer().shutdown();
    try {
      Thread.sleep(3 * 1000);   // add this sleep time to
      // ensure that the server is fully started before proceeding with tests.
    } catch (InterruptedException e) {
      // ignore
    }
    logger.info("Shutting down the kafka Server.");
    kafkaServer.stop();
    logger.info("Shutting down Zookeeper Server.");
    zookeeperServer.stopZookeeper();
    logger.info("Completed the tearDown phase.");
  }

  public String getZkUrl() {
    return zookeeperServer.getConnectString();
  }

  public String getKafkaServerUrl() {
    return "localhost:"+kafkaLocalPort;
  }
}

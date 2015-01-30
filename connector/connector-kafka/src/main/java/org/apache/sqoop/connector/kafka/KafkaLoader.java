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

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.kafka.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.kafka.configuration.LinkConfiguration;
import org.apache.sqoop.error.code.KafkaConnectorErrors;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaLoader extends Loader<LinkConfiguration,ToJobConfiguration> {
  private static final Logger LOG = Logger.getLogger(KafkaLoader.class);

  private List<KeyedMessage<String, String>> messageList =
          new ArrayList<KeyedMessage<String, String>>(KafkaConstants.DEFAULT_BATCH_SIZE);
  private Producer producer;
  private long rowsWritten = 0;

  @Override
  public void load(LoaderContext context,LinkConfiguration linkConfiguration, ToJobConfiguration jobConfiguration) throws
          Exception {
    producer = getProducer(linkConfiguration);
    LOG.info("got producer");
    String topic = jobConfiguration.toJobConfig.topic;
    LOG.info("topic is:"+topic);
    String batchUUID = UUID.randomUUID().toString();
    String record;

    while ((record = context.getDataReader().readTextRecord()) != null) {
      // create a message and add to buffer
      KeyedMessage<String, String> data = new KeyedMessage<String, String>
              (topic, null, batchUUID, record);
      messageList.add(data);
      // If we have enough messages, send the batch to Kafka
      if (messageList.size() >= KafkaConstants.DEFAULT_BATCH_SIZE) {
        sendToKafka(messageList);
      }
      rowsWritten ++;
    }

    if (messageList.size() > 0) {
      sendToKafka(messageList);
    }

    producer.close();
  }

  private void sendToKafka(List<KeyedMessage<String,String>> messageList) {
    try {
      producer.send(messageList);
      messageList.clear();
    } catch (Exception ex) {
      throw new SqoopException(KafkaConnectorErrors.KAFKA_CONNECTOR_0001);
    }
  }

  /**
   * Initialize a Kafka producer using configs in LinkConfiguration
   * @param linkConfiguration
   * @return
   */
  Producer getProducer(LinkConfiguration linkConfiguration) {
    Properties kafkaProps =  generateDefaultKafkaProps();
    kafkaProps.put(KafkaConstants.BROKER_LIST_KEY, linkConfiguration.linkConfig.brokerList);
    ProducerConfig config = new ProducerConfig(kafkaProps);
    return new Producer<String, String>(config);
  }

  /**
   * Generate producer properties object with some defaults
   * @return
   */
  private Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(KafkaConstants.MESSAGE_SERIALIZER_KEY,
            KafkaConstants.DEFAULT_MESSAGE_SERIALIZER);
    props.put(KafkaConstants.KEY_SERIALIZER_KEY,
            KafkaConstants.DEFAULT_KEY_SERIALIZER);
    props.put(KafkaConstants.REQUIRED_ACKS_KEY,
            KafkaConstants.DEFAULT_REQUIRED_ACKS);
    props.put(KafkaConstants.PRODUCER_TYPE,KafkaConstants.DEFAULT_PRODUCER_TYPE);
    return props;
  }

  /* (non-Javadoc)
   * @see org.apache.sqoop.job.etl.Loader#getRowsWritten()
   */
  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }
}

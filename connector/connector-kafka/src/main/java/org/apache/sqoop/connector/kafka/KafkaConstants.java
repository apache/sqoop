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

import org.apache.sqoop.job.Constants;

public class KafkaConstants extends Constants {
  // Resource bundle name
  public static final String RESOURCE_BUNDLE_NAME = "kafka-connector-config";

  // Kafka properties keys
  public static final String MESSAGE_SERIALIZER_KEY = "serializer.class";
  public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
  public static final String BROKER_LIST_KEY = "metadata.broker.list";
  public static final String REQUIRED_ACKS_KEY = "request.required.acks";
  public static final String PRODUCER_TYPE = "producer.type";

  // Kafka properties default values
  public static final String DEFAULT_MESSAGE_SERIALIZER =
          "kafka.serializer.StringEncoder";
  public static final String DEFAULT_KEY_SERIALIZER =
          "kafka.serializer.StringEncoder";
  public static final String DEFAULT_REQUIRED_ACKS = "-1";
  public static final String DEFAULT_PRODUCER_TYPE = "sync";
  public static final int DEFAULT_BATCH_SIZE = 100;

}

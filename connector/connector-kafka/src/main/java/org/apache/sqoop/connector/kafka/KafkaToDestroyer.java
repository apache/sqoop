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

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.kafka.configuration.ToJobConfiguration;
import org.apache.sqoop.connector.kafka.configuration.LinkConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;

public class KafkaToDestroyer extends Destroyer<LinkConfiguration, ToJobConfiguration> {

  private static final Logger LOG = Logger.getLogger(KafkaToDestroyer.class);


  public void destroy(DestroyerContext context, LinkConfiguration linkConfiguration, ToJobConfiguration jobConfiguration) {
    LOG.info("Running Kafka Connector destroyer. This does nothing except log this message.");

  }
}

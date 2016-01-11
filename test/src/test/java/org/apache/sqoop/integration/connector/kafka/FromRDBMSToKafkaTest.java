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
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.DatabaseInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KafkaInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.SqoopInfrastructureProvider;
import org.testng.annotations.Test;

@Test(groups = "no-real-cluster")
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, DatabaseInfrastructureProvider.class, KafkaInfrastructureProvider.class, SqoopInfrastructureProvider.class})
public class FromRDBMSToKafkaTest extends SqoopTestCase {

  private static final String[] input = {
    "1,'USA','2004-10-23 00:00:00.000','San Francisco'",
    "2,'USA','2004-10-24 00:00:00.000','Sunnyvale'",
    "3,'Czech Republic','2004-10-25 00:00:00.000','Brno'",
    "4,'USA','2004-10-26 00:00:00.000','Palo Alto'"
  };

  @Test
  public void testFromRDBMSToKafka() throws Exception {
    String topic = getTestName();

    createAndLoadTableCities();

    // Kafka link
    MLink kafkaLink = getClient().createLink("kafka-connector");
    fillKafkaLinkConfig(kafkaLink);
    saveLink(kafkaLink);

    // RDBMS link
    MLink rdbmsLink = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsLink);
    saveLink(rdbmsLink);

    // Job creation
    MJob job = getClient().createJob(rdbmsLink.getName(), kafkaLink.getName());

    // set rdbms "FROM" job config
    fillRdbmsFromConfig(job, "id");

    // set Kafka  "TO" job config
    fillKafkaToConfig(job, topic);

    // driver config
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);
    saveJob(job);

    executeJob(job);

    // this will assert the content of the array matches the content of the topic
    validateContent(input, topic);
  }


}

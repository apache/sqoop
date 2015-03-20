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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.KafkaConnectorTestCase;
import org.testng.annotations.Test;


public class FromRDBMSToKafkaTest extends KafkaConnectorTestCase {

  private static final String[] input = {
          "1,'USA','2004-10-23','San Francisco'",
          "2,'USA','2004-10-24','Sunnyvale'",
          "3,'Czech Republic','2004-10-25','Brno'",
          "4,'USA','2004-10-26','Palo Alto'"
  };

  @Test
  public void testBasic() throws Exception {
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
    MJob job = getClient().createJob(rdbmsLink.getPersistenceId(), kafkaLink.getPersistenceId());

    // set rdbms "FROM" job config
    fillRdbmsFromConfig(job, "id");

    // set Kafka  "TO" job config
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

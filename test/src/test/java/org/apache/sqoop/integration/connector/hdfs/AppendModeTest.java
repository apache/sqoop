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
package org.apache.sqoop.integration.connector.hdfs;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.hdfs.configuration.ToFormat;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.testng.annotations.Test;

/**
 */
public class AppendModeTest extends ConnectorTestCase {

  @Test
  public void test() throws Exception {
    createAndLoadTableCities();

    // RDBMS link
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    // HDFS link
    MLink hdfsConnection = getClient().createLink("hdfs-connector");
    fillHdfsLink(hdfsConnection);
    saveLink(hdfsConnection);

    // Job creation
    MJob job = getClient().createJob(rdbmsConnection.getPersistenceId(), hdfsConnection.getPersistenceId());

    // Set rdbms "FROM" config
    fillRdbmsFromConfig(job, "id");

    // Fill the hdfs "TO" config
    fillHdfsToConfig(job, ToFormat.TEXT_FILE);
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);


    saveJob(job);

    // First execution
    executeJob(job);
    assertTo(
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );

    // Second execution
    executeJob(job);
    assertTo(
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'",
      "1,'USA','2004-10-23','San Francisco'",
      "2,'USA','2004-10-24','Sunnyvale'",
      "3,'Czech Republic','2004-10-25','Brno'",
      "4,'USA','2004-10-26','Palo Alto'"
    );

    dropTable();
  }

}

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
package org.apache.sqoop.integration.connector.jdbc.generic;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MFormList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.test.data.Cities;
import org.apache.sqoop.test.testcases.ConnectorTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TableStagedRDBMSTest extends ConnectorTestCase {

  @Test
  public void testStagedTransfer() throws Exception {
    final String stageTableName = "STAGE_" + getTableName();
    createTableCities();
    createInputMapreduceFile("input-0001",
      "1,'USA','San Francisco'",
      "2,'USA','Sunnyvale'",
      "3,'Czech Republic','Brno'",
      "4,'USA','Palo Alto'"
    );
    new Cities(provider, stageTableName).createTables();

    // RDBMS connection
    MConnection rdbmsConnection = getClient().newConnection("generic-jdbc-connector");
    fillRdbmsConnectionForm(rdbmsConnection);
    createConnection(rdbmsConnection);

    // HDFS connection
    MConnection hdfsConnection = getClient().newConnection("hdfs-connector");
    createConnection(hdfsConnection);

    // Job creation
    MJob job = getClient().newJob(hdfsConnection.getPersistenceId(),
        rdbmsConnection.getPersistenceId());

    // Connector values
    MFormList forms = job.getConnectorPart(Direction.TO);
    forms.getStringInput("toTable.tableName").setValue(
      provider.escapeTableName(getTableName()));
    forms.getStringInput("toTable.stageTableName").setValue(
      provider.escapeTableName(stageTableName));
    fillInputForm(job);
    createJob(job);

    runJob(job);

    // @TODO(Abe): Change back after SQOOP-1488
//    assertEquals(0L, provider.rowCount(stageTableName));
//    assertEquals(4L, rowCount());
//    assertRowInCities(1, "USA", "San Francisco");
//    assertRowInCities(2, "USA", "Sunnyvale");
//    assertRowInCities(3, "Czech Republic", "Brno");
//    assertRowInCities(4, "USA", "Palo Alto");
    assertEquals(4L, provider.rowCount(stageTableName));
    assertEquals(0L, rowCount());

    // Clean up testing table
    provider.dropTable(stageTableName);
    dropTable();
  }

}

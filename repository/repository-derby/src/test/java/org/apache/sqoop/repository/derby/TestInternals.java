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
package org.apache.sqoop.repository.derby;

import java.sql.Connection;

/**
 *
 */
public class TestInternals extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new TestDerbyRepositoryHandler();
  }

  public void testSuitableInternals() throws Exception {
    assertFalse(handler.haveSuitableInternals(getDerbyConnection()));
    createSchema(); // Test code is building the structures
    assertTrue(handler.haveSuitableInternals(getDerbyConnection()));
  }

  public void testCreateorUpdateInternals() throws Exception {
    assertFalse(handler.haveSuitableInternals(getDerbyConnection()));
    handler.createOrUpdateInternals(getDerbyConnection());
    assertTrue(handler.haveSuitableInternals(getDerbyConnection()));
  }

  public void testUpgradeVersion2ToVersion4() throws Exception {
    createSchema(2);
    assertFalse(handler.haveSuitableInternals(getDerbyConnection()));
    loadConnectorAndFramework(2);
    loadConnections(2);
    loadJobs(2);
    handler.createOrUpdateInternals(getDerbyConnection());
    assertTrue(handler.haveSuitableInternals(getDerbyConnection()));
  }

  private class TestDerbyRepositoryHandler extends DerbyRepositoryHandler {
    protected long registerHdfsConnector(Connection conn) {
      try {
        runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
            + "VALUES('hdfs-connector', 'org.apache.sqoop.test.B', '1.0-test')");
        return 2L;
      } catch(Exception e) {
        return -1L;
      }
    }
  }
}

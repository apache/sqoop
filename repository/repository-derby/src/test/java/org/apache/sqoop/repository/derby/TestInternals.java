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

import org.apache.sqoop.common.SqoopException;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestInternals extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    handler = new TestDerbyRepositoryHandler();
  }

  @Test
  public void testSuitableInternals() throws Exception {
    assertFalse(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
    createSchema(); // Test code is building the structures
    assertTrue(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
  }

  @Test
  public void testCreateorUpdateInternals() throws Exception {
    assertFalse(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
    handler.createOrUpdateInternals(getDerbyDatabaseConnection());
    assertTrue(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
  }

  @Test(expected=SqoopException.class)
  public void testUpgradeVersion2ToVersion4DuplicateFailure() throws Exception {
    createSchema(2);
    assertFalse(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
    loadConnectorAndDriverConfig(2);
    loadLinks(2);
    loadJobs(2);
    handler.createOrUpdateInternals(getDerbyDatabaseConnection());
    assertTrue(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
  }

  @Test
  public void testUpgradeVersion2ToVersion4() throws Exception {
    createSchema(2);
    assertFalse(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
    loadConnectorAndDriverConfig(2);
    loadLinks(2);
    loadJobs(2);
    renameLinks();
    renameJobs();
    handler.createOrUpdateInternals(getDerbyDatabaseConnection());
    assertTrue(handler.haveSuitableInternals(getDerbyDatabaseConnection()));
  }

  private Map<String, List<Long>> getNameIdMap(PreparedStatement ps) throws SQLException {
    Map<String, List<Long>> nameIdMap = new TreeMap<String, List<Long>>();
    ResultSet rs = null;

    try {
      rs = ps.executeQuery();
      while(rs.next()) {
        if (!nameIdMap.containsKey(rs.getString(1))) {
          nameIdMap.put(rs.getString(1), new LinkedList<Long>());
        }
        nameIdMap.get(rs.getString(1)).add(rs.getLong(2));
      }
    } finally {
      if(rs != null) {
        rs.close();
      }
      if(ps != null) {
        ps.close();
      }
    }

    return nameIdMap;
  }

  private void renameLinks() throws Exception {
    Map<String, List<Long>> nameIdMap =
        getNameIdMap(getDerbyDatabaseConnection().prepareStatement("SELECT SQ_LNK_NAME, SQ_LNK_ID FROM SQOOP.SQ_LINK"));;

    for (String name : nameIdMap.keySet()) {
      if (nameIdMap.get(name).size() > 1) {
        for (Long id : nameIdMap.get(name)) {
          runQuery("UPDATE SQOOP.SQ_LINK SET SQ_LNK_NAME=? WHERE SQ_LNK_ID=?", name + "-" + id, id);
        }
      }
    }
  }

  private void renameJobs() throws Exception {
    Map<String, List<Long>> nameIdMap =
        getNameIdMap(getDerbyDatabaseConnection().prepareStatement("SELECT SQB_NAME, SQB_ID FROM SQOOP.SQ_JOB"));;

    for (String name : nameIdMap.keySet()) {
      if (nameIdMap.get(name).size() > 1) {
        for (Long id : nameIdMap.get(name)) {
          runQuery("UPDATE SQOOP.SQ_JOB SET SQB_NAME=? WHERE SQB_ID=?", name + "-" + id, id);
        }
      }
    }
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

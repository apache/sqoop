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

package org.apache.sqoop.accumulo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Utility methods that facilitate Accumulo import tests.
 * These test use the MiniAccumuloCluster.  They are
 * absolutely not thread safe.
 */
public abstract class AccumuloTestCase extends ImportJobTestCase {
  private static final String ACCUMULO_USER="root";
  private static final String ACCUMULO_PASSWORD="rootroot";

  /*
   * This is to restore test.build.data system property which gets reset
   * when Accumulo tests are run. Since other tests in Sqoop also depend upon
   * this property, they can fail if are run subsequently in the same VM.
   */
  private static String testBuildDataProperty = "";

  private static void recordTestBuildDataProperty() {
    testBuildDataProperty = System.getProperty("test.build.data", "");
  }

  private static void restoreTestBuidlDataProperty() {
    System.setProperty("test.build.data", testBuildDataProperty);
  }

  public static final Log LOG = LogFactory.getLog(
      AccumuloTestCase.class.getName());

  protected static MiniAccumuloCluster accumuloCluster;
  protected static File tempDir;

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(String accumuloTable,
      String accumuloColFam, boolean accumuloCreate,
      String queryStr) {

    ArrayList<String> args = new ArrayList<String>();

    if (null != queryStr) {
      args.add("--query");
      args.add(queryStr);
    } else {
      args.add("--table");
      args.add(getTableName());
    }
    args.add("--split-by");
    args.add(getColName(0));
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--num-mappers");
    args.add("1");
    args.add("--accumulo-column-family");
    args.add(accumuloColFam);
    args.add("--accumulo-table");
    args.add(accumuloTable);
    if (accumuloCreate) {
      args.add("--accumulo-create-table");
    }
    args.add("--accumulo-instance");
    args.add(accumuloCluster.getInstanceName());
    args.add("--accumulo-zookeepers");
    args.add(accumuloCluster.getZooKeepers());
    args.add("--accumulo-user");
    args.add(ACCUMULO_USER);
    args.add("--accumulo-password");
    args.add(ACCUMULO_PASSWORD);

    return args.toArray(new String[0]);
  }

  protected static void setUpCluster() throws Exception {
    File temp = File.createTempFile("test", "tmp");
    tempDir = new File(temp.getParent(), "accumulo"
      + System.currentTimeMillis());
    tempDir.mkdir();
    tempDir.deleteOnExit();
    temp.delete();
    accumuloCluster = new MiniAccumuloCluster(tempDir, ACCUMULO_PASSWORD);
    accumuloCluster.start();
  }

  protected static void cleanUpCluster() throws Exception {
    accumuloCluster.stop();
    delete(tempDir);
  }

  protected static void delete(File dir) {
    if (dir.isDirectory()) {
      File[] kids = dir.listFiles();
      for (File f : kids) {
        if (f.isDirectory()) {
          delete(f);
        } else {
          f.delete();
        }
      }
    }
    dir.delete();
  }

  @Override
  @Before
  public void setUp() {
    try {
      setUpCluster();
    } catch (Exception e) {
      LOG.error("Error setting up MiniAccumuloCluster.", e);
    }
    AccumuloTestCase.recordTestBuildDataProperty();
    super.setUp();
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    try {
      cleanUpCluster();
    } catch (Exception e) {
      LOG.error("Error stopping MiniAccumuloCluster.", e);
    }
  }

  protected void verifyAccumuloCell(String tableName, String rowKey,
      String colFamily, String colName, String val) throws IOException {
    try {
      Instance inst = new ZooKeeperInstance(accumuloCluster.getInstanceName(),
        accumuloCluster.getZooKeepers());
      Connector conn = inst.getConnector(ACCUMULO_USER,
        new PasswordToken(ACCUMULO_PASSWORD));
      Scanner scanner = conn.createScanner(tableName, Constants.NO_AUTHS);
      scanner.setRange(new Range(rowKey));
      Iterator<Entry<Key, Value>> iter = scanner.iterator();
      while (iter.hasNext()) {
        Entry<Key, Value> entry = iter.next();
        String columnFamily = entry.getKey().getColumnFamily().toString();
        String qual = entry.getKey().getColumnQualifier().toString();
        if (columnFamily.equals(colFamily)
            && qual.equals(colName)) {
          String value = entry.getValue().toString();
          if (null == val) {
            assertNull("Got a result when expected null", value);
          } else {
            assertNotNull("No result, but we expected one", value);
            assertEquals(val, value);
          }
        }
      }
    } catch (AccumuloException e) {
      throw new IOException("AccumuloException in verifyAccumuloCell", e);
    } catch (AccumuloSecurityException e) {
      throw new IOException("AccumuloSecurityException in verifyAccumuloCell",
          e);
    } catch (TableNotFoundException e) {
      throw new IOException("TableNotFoundException in verifyAccumuloCell", e);
    }
  }
}

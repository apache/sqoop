/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.util.StringUtils;

import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Utility methods that facilitate HBase import tests.
 */
public class HBaseTestCase extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(
      HBaseTestCase.class.getName());

  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags,
      String hbaseTable, String hbaseColFam, boolean hbaseCreate,
      String queryStr) {

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

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
    args.add("--column-family");
    args.add(hbaseColFam);
    args.add("--hbase-table");
    args.add(hbaseTable);
    if (hbaseCreate) {
      args.add("--hbase-create-table");
    }

    return args.toArray(new String[0]);
  }

  // Starts a mini hbase cluster in this process.
  private HBaseTestingUtility hbaseTestUtil;

  private void startMaster() throws Exception {
    if (null == hbaseTestUtil) {
      Configuration conf = new Configuration();
      conf = HBaseConfiguration.addHbaseResources(conf);
      hbaseTestUtil = new HBaseTestingUtility(conf);
      hbaseTestUtil.startMiniCluster(1);
    }
  }

  @Override
  @Before
  public void setUp() {
    try {
      startMaster();
    } catch (Exception e) {
      fail(e.toString());
    }
    super.setUp();
  }


  public void shutdown() throws Exception {
    LOG.info("In shutdown() method");
    if (null != hbaseTestUtil) {
      LOG.info("Shutting down HBase cluster");
      hbaseTestUtil.shutdownMiniCluster();
      this.hbaseTestUtil = null;
    }
    LOG.info("shutdown() method returning."); 
  }

  @Override
  @After
  public void tearDown() {
    try {
      shutdown();
    } catch (Exception e) {
      LOG.warn("Error shutting down HBase minicluster: "
          + StringUtils.stringifyException(e));
    }

    super.tearDown();
  }

  protected void verifyHBaseCell(String tableName, String rowKey,
      String colFamily, String colName, String val) throws IOException {
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
    HTable table = new HTable(Bytes.toBytes(tableName));
    try {
      Result r = table.get(get);
      byte [] actualVal = r.getValue(Bytes.toBytes(colFamily),
          Bytes.toBytes(colName));
      if (null == val) {
        assertNull("Got a result when expected null", actualVal);
      } else {
        assertNotNull("No result, but we expected one", actualVal);
        assertEquals(val, Bytes.toString(actualVal));
      }
    } finally {
      table.close();
    }
  }
}

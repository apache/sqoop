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

package com.cloudera.sqoop.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.util.StringUtils;

import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import java.io.File;
import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.commons.io.FileUtils;

/**
 * Utility methods that facilitate HBase import tests.
 */
public abstract class HBaseTestCase extends ImportJobTestCase {

  /*
   * This is to restore test.build.data system property which gets reset
   * when HBase tests are run. Since other tests in Sqoop also depend upon
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
      args.add("-D");
      args.add("hbase.zookeeper.property.clientPort=21818");
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
  // Starts a mini hbase cluster in this process.
  private HBaseTestingUtility hbaseTestUtil;
  private String workDir = createTempDir().getAbsolutePath();
  private MiniZooKeeperCluster zookeeperCluster;
  private MiniHBaseCluster hbaseCluster;

  @Override
  @Before
  public void setUp() {
    try {
      HBaseTestCase.recordTestBuildDataProperty();
      String hbaseDir = new File(workDir, "hbase").getAbsolutePath();
      String hbaseRoot = "file://" + hbaseDir;
      Configuration hbaseConf = HBaseConfiguration.create();
      hbaseConf.set(HConstants.HBASE_DIR, hbaseRoot);
      //Hbase 0.90 does not have HConstants.ZOOKEEPER_CLIENT_PORT
      hbaseConf.setInt("hbase.zookeeper.property.clientPort", 21818);
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "0.0.0.0");
      hbaseConf.setInt("hbase.master.info.port", -1);
      hbaseConf.setInt("hbase.zookeeper.property.maxClientCnxns", 500);
      String zookeeperDir = new File(workDir, "zk").getAbsolutePath();
      int zookeeperPort = 21818;
      zookeeperCluster = new MiniZooKeeperCluster();
      Method m;
      Class<?> zkParam[] = {Integer.TYPE};
      try {
        m = MiniZooKeeperCluster.class.getDeclaredMethod("setDefaultClientPort",
                zkParam);
      } catch (NoSuchMethodException e) {
        m = MiniZooKeeperCluster.class.getDeclaredMethod("setClientPort",
                zkParam);
      }
      m.invoke(zookeeperCluster, new Object[]{new Integer(zookeeperPort)});
      zookeeperCluster.startup(new File(zookeeperDir));
      hbaseCluster = new MiniHBaseCluster(hbaseConf, 1);
      HMaster master = hbaseCluster.getMaster();
      Object serverName = master.getServerName();

      String hostAndPort;
      if (serverName instanceof String) {
        System.out.println("Server name is string, using HServerAddress.");
        m = HMaster.class.getDeclaredMethod("getMasterAddress",
                new Class<?>[]{});
        Class<?> clazz = Class.forName("org.apache.hadoop.hbase.HServerAddress");
        /*
         * Call method to get server address
         */
        Object serverAddr = clazz.cast(m.invoke(master, new Object[]{}));
        //returns the address as hostname:port
        hostAndPort = serverAddr.toString();
      } else {
        System.out.println("ServerName is org.apache.hadoop.hbase.ServerName,"
                + "using getHostAndPort()");
        Class<?> clazz = Class.forName("org.apache.hadoop.hbase.ServerName");
        m = clazz.getDeclaredMethod("getHostAndPort", new Class<?>[]{});
        hostAndPort = m.invoke(serverName, new Object[]{}).toString();
      }
      hbaseConf.set("hbase.master", hostAndPort);
      hbaseTestUtil = new HBaseTestingUtility(hbaseConf);
      hbaseTestUtil.setZkCluster(zookeeperCluster);
      hbaseCluster.startMaster();
      super.setUp();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() throws Exception {
    LOG.info("In shutdown() method");
    if (null != hbaseTestUtil) {
      LOG.info("Shutting down HBase cluster");
      hbaseCluster.shutdown();
      zookeeperCluster.shutdown();
      hbaseTestUtil = null;
    }
    FileUtils.deleteDirectory(new File(workDir));
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
    HBaseTestCase.restoreTestBuidlDataProperty();
    super.tearDown();
  }

  protected void verifyHBaseCell(String tableName, String rowKey,
      String colFamily, String colName, String val) throws IOException {
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
    HTable table = new HTable(new Configuration(
        hbaseTestUtil.getConfiguration()), Bytes.toBytes(tableName));
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
  public static File createTempDir() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File tempDir = new File(baseDir, UUID.randomUUID().toString());
    if (tempDir.mkdir()) {
      return tempDir;
    }
    throw new IllegalStateException("Failed to create directory");
  }

  protected int countHBaseTable(String tableName, String colFamily)
      throws IOException {
    int count = 0;
    HTable table = new HTable(new Configuration(
        hbaseTestUtil.getConfiguration()), Bytes.toBytes(tableName));
    try {
      ResultScanner scanner = table.getScanner(Bytes.toBytes(colFamily));
      for(Result result = scanner.next();
          result != null;
          result = scanner.next()) {
        count++;
      }
    } finally {
      table.close();
    }
    return count;
  }
}

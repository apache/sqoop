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

package com.cloudera.sqoop.phoenix;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.util.StringUtils;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Before;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * 
 * Base test class for all phoenix tests.
 *
 */
public abstract class PhoenixTestCase extends ImportJobTestCase {

	public static final Log LOG = LogFactory.getLog(
		PhoenixTestCase.class.getName());
	 
	private static String testBuildDataProperty = "";

	private static void recordTestBuildDataProperty() {
		testBuildDataProperty = System.getProperty("test.build.data", "");
	}

	private static void restoreTestBuidlDataProperty() {
		System.setProperty("test.build.data", testBuildDataProperty);
	}
	
	private HBaseTestingUtility hbaseTestUtil;
	private String workDir = createTempDir().getAbsolutePath();
	private MiniZooKeeperCluster zookeeperCluster;
	private MiniHBaseCluster hbaseCluster;
	private static String zkQuorum;
	protected static Connection conn;

	@Override
	@Before
	public void setUp() {
		try {
	    	PhoenixTestCase.recordTestBuildDataProperty();
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
	      Class.forName(PhoenixDriver.class.getName());
	      zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
	      conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL
	                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum);
	      super.setUp();
	    } catch (Throwable e) {
	      throw new RuntimeException(e);
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
	
	/**
	 * creates the test table given the ddl
	 * @param url
	 * @param ddl
	 * @throws SQLException
	 */
	protected static void createTestTable(String ddl) throws SQLException {
        assertNotNull(ddl);
        try {
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute(ddl);
        } catch (TableAlreadyExistsException e) {
            // ignore if already exists
        } 
    }
	  
	public void shutdown() throws Exception {
		LOG.info("In shutdown() method");
	    if (null != hbaseTestUtil) {
	    	LOG.info("Shutting down HBase cluster");
	    	hbaseCluster.shutdown();
	    	zookeeperCluster.shutdown();
	    	hbaseTestUtil = null;
	    	conn.close();
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
	    PhoenixTestCase.restoreTestBuidlDataProperty();
	    super.tearDown();
	}
	
  /**
   * Create the argv to pass to Sqoop.
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags,
      String phoenixTable, String phoenixColumnMapping,String queryStr) {

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
    args.add("--phoenix-table");
    args.add(phoenixTable);
    if(null != phoenixColumnMapping) {
    	args.add("--phoenix-column-mapping");
    	args.add(phoenixColumnMapping);
    }
   
    return args.toArray(new String[0]);
  }
}

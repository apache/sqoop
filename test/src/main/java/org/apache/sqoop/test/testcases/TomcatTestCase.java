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
package org.apache.sqoop.test.testcases;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.test.asserts.HdfsAsserts;
import org.apache.sqoop.test.hadoop.HadoopRunner;
import org.apache.sqoop.test.hadoop.HadoopRunnerFactory;
import org.apache.sqoop.test.hadoop.HadoopLocalRunner;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.minicluster.TomcatSqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.ITest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

/**
 * Basic test case that will bootstrap Sqoop server running in external Tomcat
 * process.
 */
abstract public class TomcatTestCase {
  private static final Logger LOG = Logger.getLogger(TomcatTestCase.class);

  public String name;

  /**
   * Temporary base path that will be used for tests.
   *
   * By default we will take a look for sqoop.integration.tmpdir property that is
   * filled up by maven. If the test is not started from maven (IDE) we will
   * pick up configured java.io.tmpdir value. The last results is /tmp/ directory
   * in case that no property is set.
   */
  protected static final String TMP_PATH_BASE =
    System.getProperty("sqoop.integration.tmpdir", System.getProperty("java.io.tmpdir", "/tmp")) + "/sqoop-cargo-tests/";

  /**
   * Temporary directory that will be used by the test.
   *
   * We will take TMP_PATH_BASE and append two subdirectories. First will be named
   * after fully qualified class name of current test class, second directory will
   * be named after current test method name. For example:
   *
   * TMP_PATH_BASE/org.apache.sqoop.TestClass/testMethod/
   */
  private String tmpPath;

  /**
   * Hadoop cluster
   */
  protected static HadoopRunner hadoopCluster;

  /**
   * Hadoop client
   */
  protected static FileSystem hdfsClient;

  /**
   * Tomcat based Sqoop mini cluster
   */
  private TomcatSqoopMiniCluster cluster;

  /**
   * Sqoop client API.
   */
  private SqoopClient client;

  @BeforeSuite(alwaysRun = true)
  public static void startHadoop() throws Exception {
    // Start Hadoop Clusters
    hadoopCluster = HadoopRunnerFactory.getHadoopCluster(System.getProperties(), HadoopLocalRunner.class);
    hadoopCluster.setTemporaryPath(TMP_PATH_BASE);
    hadoopCluster.setConfiguration( hadoopCluster.prepareConfiguration(new JobConf()) );
    hadoopCluster.start();

    // Initialize Hdfs Client
    hdfsClient = FileSystem.get(hadoopCluster.getConfiguration());
    LOG.debug("HDFS Client: " + hdfsClient);
  }

  @BeforeMethod(alwaysRun = true)
  public void findMethodName(Method method) {
    if(this instanceof ITest) {
      name = ((ITest)this).getTestName();
    } else {
      name = method.getName();
    }
  }

  @BeforeMethod(alwaysRun = true)
  public void startServer() throws Exception {
    // Get and set temporary path in hadoop cluster.
    tmpPath = HdfsUtils.joinPathFragments(TMP_PATH_BASE, getClass().getName(), name);
    FileUtils.deleteDirectory(new File(tmpPath));

    LOG.debug("Temporary Directory: " + tmpPath);

    // Start server
    cluster = createSqoopMiniCluster();
    cluster.start();

    // Initialize Sqoop Client API
    client = new SqoopClient(getServerUrl());
  }

  @AfterMethod(alwaysRun = true)
  public void stopServer() throws Exception {
    cluster.stop();
  }

  @AfterSuite(alwaysRun = true)
  public static void stopHadoop() throws Exception {
    hadoopCluster.stop();
  }

  /**
   * Create Sqoop MiniCluster instance that should be used for this test.
   *
   * This method will be executed only once prior each test execution.
   *
   * @return New instance of test mini cluster
   */
  public TomcatSqoopMiniCluster createSqoopMiniCluster() throws Exception {
    return new TomcatSqoopMiniCluster(getSqoopMiniClusterTemporaryPath(), hadoopCluster.getConfiguration());
  }

  /**
   * Return SqoopClient configured to talk to testing server.
   *
   * @return
   */
  public SqoopClient getClient() {
    return client;
  }

  public SqoopMiniCluster getCluster() {
    return cluster;
  }

  public String getTemporaryPath() {
    return tmpPath;
  }

  public String getSqoopMiniClusterTemporaryPath() {
    return HdfsUtils.joinPathFragments(tmpPath, "sqoop-mini-cluster");
  }

  /**
   * Return testing server URL
   *
   * @return
   */
  public String getServerUrl() {
    return cluster.getServerUrl();
  }

  /**
   * Return mapreduce base directory.
   *
   * @return
   */
  public String getMapreduceDirectory() {
    return HdfsUtils.joinPathFragments(hadoopCluster.getTestDirectory(), getClass().getName(), name);
  }

  /**
   * Assert that execution has generated following lines.
   *
   * As the lines can be spread between multiple files the ordering do not make
   * a difference.
   *
   * @param lines
   * @throws IOException
   */
  protected void assertTo(String... lines) throws IOException {
    // TODO(VB): fix this to be not directly dependent on hdfs/MR
    HdfsAsserts.assertMapreduceOutput(hdfsClient, getMapreduceDirectory(), lines);
  }

  /**
   * Verify number of TO files.
   *
   * @param expectedFiles Expected number of files
   */
  protected void assertToFiles(int expectedFiles) throws IOException {
    // TODO(VB): fix this to be not directly dependent on hdfs/MR
    HdfsAsserts.assertMapreduceOutputFiles(hdfsClient, getMapreduceDirectory(), expectedFiles);
  }

  /**
   * Create FROM file with specified content.
   *
   * @param filename Input file name
   * @param lines Individual lines that should be written into the file
   * @throws IOException
   */
  protected void createFromFile(String filename, String...lines) throws IOException {
    HdfsUtils.createFile(hdfsClient, HdfsUtils.joinPathFragments(getMapreduceDirectory(), filename), lines);
  }
}

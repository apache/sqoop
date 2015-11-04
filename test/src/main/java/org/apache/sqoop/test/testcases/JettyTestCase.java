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
import org.apache.sqoop.test.hadoop.HadoopMiniClusterRunner;
import org.apache.sqoop.test.hadoop.HadoopRunner;
import org.apache.sqoop.test.hadoop.HadoopRunnerFactory;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.ITest;
import org.testng.ITestContext;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

/**
 * Basic test case that will bootstrap Sqoop server running in embedded Jetty
 * process.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"MS_PKGPROTECT", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
abstract public class JettyTestCase implements ITest {
  private static final Logger LOG = Logger.getLogger(JettyTestCase.class);

  public String methodName;

  /**
   * Temporary base path that will be used for tests.
   *
   * By default we will take a look for sqoop.integration.tmpdir property that is
   * filled up by maven. If the test is not started from maven (IDE) we will
   * pick up configured java.io.tmpdir value. The last results is /tmp/ directory
   * in case that no property is set.
   */
  protected static final String TMP_PATH_BASE =
    System.getProperty("sqoop.integration.tmpdir", System.getProperty("java.io.tmpdir", "/tmp")) + "/sqoop-cargo-tests";

  /**
   * Temporary directory that will be used by the test.
   *
   * We will take TMP_PATH_BASE and append the test suite. For example:
   *
   * TMP_PATH_BASE/TestConnectorsSuite
   */
  private static String tmpPath;

  /**
   * Hadoop cluster
   */
  protected static HadoopRunner hadoopCluster;

  /**
   * Hadoop client
   */
  protected static FileSystem hdfsClient;

  /**
   * Jetty based Sqoop mini cluster
   */
  private static JettySqoopMiniCluster cluster;

  /**
   * Sqoop client API.
   */
  private static SqoopClient client;

  /**
   * Use the method name as the test name
   */
  public String getTestName() {
    return methodName;
  }

  @BeforeMethod(alwaysRun = true)
  public void setMethodName(Method method) throws Exception {
    methodName = method.getName();
  }

  @BeforeSuite(alwaysRun = true)
  public void setupSuite(ITestContext context) throws Exception {
    tmpPath = HdfsUtils.joinPathFragments(TMP_PATH_BASE, context.getSuite().getName());

    LOG.debug("Temporary Directory: " + getTemporaryPath());
    FileUtils.deleteDirectory(new File(getTemporaryPath()));

    startHadoop();
    startSqoop();
  }

  @AfterSuite(alwaysRun = true)
  public void tearDownSuite() throws Exception {
    stopSqoop();
    stopHadoop();
  }

  protected void startHadoop() throws Exception {
    // Start Hadoop Clusters
    hadoopCluster = HadoopRunnerFactory.getHadoopCluster(System.getProperties(), HadoopMiniClusterRunner.class);
    hadoopCluster.setTemporaryPath(getTemporaryPath());
    hadoopCluster.setConfiguration(hadoopCluster.prepareConfiguration(new JobConf()));
    hadoopCluster.start();

    // Initialize Hdfs Client
    hdfsClient = FileSystem.get(hadoopCluster.getConfiguration());
    LOG.debug("HDFS Client: " + hdfsClient);
  }

  protected void startSqoop() throws Exception {
    // Start server
    cluster = createSqoopMiniCluster();
    cluster.start();

    // Initialize Sqoop Client API
    client = new SqoopClient(getServerUrl());
  }

  protected void stopSqoop() throws Exception {
    if (cluster != null) {
      cluster.stop();
    }
  }

  protected void stopHadoop() throws Exception {
    hadoopCluster.stop();
  }

  /**
   * Create Sqoop MiniCluster instance that should be used for this test.
   *
   * @return New instance of test mini cluster
   */
  public JettySqoopMiniCluster createSqoopMiniCluster() throws Exception {
    return new JettySqoopMiniCluster(getSqoopMiniClusterTemporaryPath(), hadoopCluster.getConfiguration());
  }

  /**
   * Return SqoopClient configured to talk to testing server.
   *
   * @return
   */
  public static SqoopClient getClient() {
    return client;
  }

  public static void setClient(SqoopClient sqoopClient) {
    client = sqoopClient;
  }

  public static SqoopMiniCluster getCluster() {
    return cluster;
  }

  public static void setCluster(JettySqoopMiniCluster sqoopMiniClusterluster) {
    cluster = sqoopMiniClusterluster;
  }

  public static String getTemporaryPath() {
    return tmpPath;
  }

  public static String getSqoopMiniClusterTemporaryPath() {
    return HdfsUtils.joinPathFragments(getTemporaryPath(), "sqoop-mini-cluster");
  }

  /**
   * Return testing server URL
   *
   * @return
   */
  public static String getServerUrl() {
    return cluster.getServerUrl();
  }

  /**
   * Return mapreduce base directory.
   *
   * @return
   */
  public String getMapreduceDirectory() {
    return HdfsUtils.joinPathFragments(hadoopCluster.getTestDirectory(), getClass().getName(), getTestName());
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

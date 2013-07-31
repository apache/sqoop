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

import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.test.asserts.HdfsAsserts;
import org.apache.sqoop.test.minicluster.TomcatSqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;

/**
 * Basic test case that will bootstrap Sqoop server running in external Tomcat
 * process.
 */
abstract public class TomcatTestCase {

  private static final Logger LOG = Logger.getLogger(TomcatTestCase.class);

  @Rule public TestName name = new TestName();

  /**
   * Temporary base path that will be used for tests.
   *
   * By default we will take a look for sqoop.integration.tmpdir property that is
   * filled up by maven. If the test is not started from maven (IDE) we will
   * pick up configured java.io.tmpdir value. The last results is /tmp/ directory
   * in case that no property is set.
   */
  private static final String TMP_PATH_BASE =
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
   * Tomcat based Sqoop mini cluster
   */
  private TomcatSqoopMiniCluster cluster;

  /**
   * Sqoop client API.
   */
  private SqoopClient client;

  @Before
  public void startServer() throws Exception {
    // Set up the temporary path
    tmpPath = TMP_PATH_BASE + getClass().getName() + "/" + name.getMethodName() + "/";

    // Set up and start server
    cluster = new TomcatSqoopMiniCluster(getTemporaryPath());
    cluster.start();

    // Initialize Sqoop Client API
    client = new SqoopClient(getServerUrl());
  }

  @After
  public void stopServer() throws Exception {
    cluster.stop();
  }

  /**
   * Return SqoopClient configured to talk to testing server.
   *
   * @return
   */
  public SqoopClient getClient() {
    return client;
  }

  public String getTemporaryPath() {
    return tmpPath;
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
   * Get input/output directory for mapreduce job.
   *
   * @return
   */
  public String getMapreduceDirectory() {
    return getTemporaryPath() + "/mapreduce-job-io";
  }

  /**
   * Assert that mapreduce has generated following lines.
   *
   * As the lines can be spread between multiple files the ordering do not make
   * a difference.
   *
   * @param lines
   * @throws IOException
   */
  protected void assertMapreduceOutput(String... lines) throws IOException {
    HdfsAsserts.assertMapreduceOutput(getMapreduceDirectory(), lines);
  }

  /**
   * Verify number of output mapreduce files.
   *
   * @param expectedFiles Expected number of files
   */
  protected void assertMapreduceOutputFiles(int expectedFiles) {
    HdfsAsserts.assertMapreduceOutputFiles(getMapreduceDirectory(), expectedFiles);
  }

  /**
   * Create mapreduce input file with specified content.
   *
   * @param filename Input file name
   * @param lines Individual lines that should be written into the file
   * @throws IOException
   */
  protected void createInputMapreduceFile(String filename, String...lines) throws IOException {
    HdfsUtils.createFile(getMapreduceDirectory(), filename, lines);
  }
}

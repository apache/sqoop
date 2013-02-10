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
package org.apache.sqoop.integration;

import org.apache.sqoop.test.minicluster.TomcatSqoopMiniCluster;
import org.junit.After;
import org.junit.Before;

/**
 * Basic test case that will bootstrap Sqoop server running in external Tomcat
 * process.
 */
abstract public class TomcatTestCase {

  /**
   * Temporary path that will be used for this test.
   *
   * By default we will take look for sqoop.integration.tmpdir property that is
   * filled up by maven. If the test is not started from maven (IDE) we will
   * pick up configured java.io.tmpdir value. The last results is /tmp/ directory
   * in case that no property is set.
   */
  private final String TMP_PATH =
    System.getProperty("sqoop.integration.tmpdir", System.getProperty("java.io.tmpdir", "/tmp"))
      + "/sqoop-cargo-tests/" + getClass().getName() + "/";

  /**
   * Tomcat based Sqoop mini cluster
   */
  private TomcatSqoopMiniCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new TomcatSqoopMiniCluster(TMP_PATH);
    cluster.start();
  }

  @After
  public void cleanUp() throws Exception {
    cluster.stop();
  }

  /**
   * Return testing server URL
   *
   * @return
   */
  public String getServerUrl() {
    return cluster.getServerUrl();
  }
}

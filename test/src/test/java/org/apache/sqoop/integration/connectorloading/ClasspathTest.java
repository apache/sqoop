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

package org.apache.sqoop.integration.connectorloading;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.testcases.ConnectorClasspathTestCase;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = "no-real-cluster")
public class ClasspathTest extends ConnectorClasspathTestCase {

  private static final String TEST_CONNECTOR_JAR_NAME = "test-connector.jar";
  private static final String TEST_DEPENDENCY_JAR_NAME = "test-dependency.jar";

  private static final String[] CONNECTOR_SOURCE_FILES = {
    "TestConnector/TestConnector.java",
    "TestConnector/TestLinkConfiguration.java",
    "TestConnector/TestLoader.java",
    "TestConnector/TestToDestroyer.java",
    "TestConnector/TestToInitializer.java",
    "TestConnector/TestToJobConfiguration.java"
  };

  private static final String[] CONNECTOR_DEPENDENCY_SOURCE_FILES = {
    "TestConnector/TestDependency.java"
  };

  private static final String[] CONNECTOR_PROPERTY_FILES = {
    "TestConnector/sqoopconnector.properties"
  };

  private ClassLoader classLoader;

  public static class DerbySqoopMiniCluster extends JettySqoopMiniCluster {

    private String extraClasspath;
    private String jobExtraClasspath;

    public DerbySqoopMiniCluster(String temporaryPath, Configuration configuration, String extraClasspath, String jobExtraClasspath) throws Exception {
      super(temporaryPath, configuration);
      this.extraClasspath = extraClasspath;
      this.jobExtraClasspath = jobExtraClasspath;
    }

    @Override
    protected Map<String, String> getClasspathConfiguration() {
      Map<String, String> properties = new HashMap<>();

      if (extraClasspath != null) {
        properties.put(ConfigurationConstants.CLASSPATH, extraClasspath);
      }
      if (jobExtraClasspath != null) {
        properties.put(ConfigurationConstants.JOB_CLASSPATH, jobExtraClasspath);
      }


      return properties;
    }
  }

  public void startSqoopMiniCluster(String extraClasspath, String jobExtraClasspath) throws Exception {
    // And use them for new Derby repo instance
    setCluster(new DerbySqoopMiniCluster(HdfsUtils.joinPathFragments(super.getSqoopMiniClusterTemporaryPath(), getTestName()), hadoopCluster.getConfiguration(), extraClasspath, jobExtraClasspath));

    // Start server
    getCluster().start();

    // Initialize Sqoop Client API
    setClient(new SqoopClient(getServerUrl()));
  }

  @BeforeMethod
  public void captureClasspath() {
    classLoader = Thread.currentThread().getContextClassLoader();
  }

  @AfterMethod
  public void restoreClasspath(){
    Thread.currentThread().setContextClassLoader(classLoader);
  }

  @Test
  public void testClasspathSqoopProperties() throws Exception {
    Map<String, String> jarMap = compileTestConnectorAndDependency(
        CONNECTOR_SOURCE_FILES,
        CONNECTOR_DEPENDENCY_SOURCE_FILES,
        CONNECTOR_PROPERTY_FILES,
        TEST_CONNECTOR_JAR_NAME,
        TEST_DEPENDENCY_JAR_NAME,
        false);
    startSqoopMiniCluster(jarMap.get(TEST_CONNECTOR_JAR_NAME), jarMap.get
      (TEST_DEPENDENCY_JAR_NAME));
    createAndLoadTableCities();

    MJob job = prepareJob();

    prepareDriverConfig(job);

    saveJob(job);

    executeJob(job);

    stopSqoop();
    deleteJars(jarMap);
  }

  @Test
  public void testClasspathDriverInput() throws Exception{
    Map<String, String> jarMap = compileTestConnectorAndDependency(
        CONNECTOR_SOURCE_FILES,
        CONNECTOR_DEPENDENCY_SOURCE_FILES,
        CONNECTOR_PROPERTY_FILES,
        TEST_CONNECTOR_JAR_NAME,
        TEST_DEPENDENCY_JAR_NAME,
        false);
    startSqoopMiniCluster(jarMap.get(TEST_CONNECTOR_JAR_NAME), null);
    createAndLoadTableCities();

    MJob job = prepareJob();

    MDriverConfig driverConfig = prepareDriverConfig(job);

    List<String> extraJars = new ArrayList<>();
    extraJars.add("file:" + jarMap.get(TEST_DEPENDENCY_JAR_NAME));
    driverConfig.getListInput("jarConfig.extraJars").setValue(extraJars);

    saveJob(job);

    executeJob(job);

    stopSqoop();
    deleteJars(jarMap);
  }

  private MJob prepareJob() {
    MLink rdbmsConnection = getClient().createLink("generic-jdbc-connector");
    fillRdbmsLinkConfig(rdbmsConnection);
    saveLink(rdbmsConnection);

    MLink testConnection = getClient().createLink("test-connector");
    saveLink(testConnection);

    MJob job = getClient().createJob(rdbmsConnection.getName(), testConnection.getName());

    fillRdbmsFromConfig(job, "id");

    return job;
  }

  private MDriverConfig prepareDriverConfig(MJob job) {
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    return driverConfig;
  }

  @Override
  public void startSqoop() throws Exception {
    // Do nothing so that Sqoop isn't started before Suite.
  }
}

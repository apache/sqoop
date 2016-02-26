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

package org.apache.sqoop.integration.serverproperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.ConnectorUtils;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "no-real-cluster")
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class})
public class ConnectorClasspathIsolationTest extends SqoopTestCase {

  private static final String TEST_FROM_CONNECTOR_JAR_NAME = "test-from-connector.jar";
  private static final String TEST_TO_CONNECTOR_JAR_NAME = "test-to-connector.jar";
  private static final String TEST_FROM_DEPENDENCY_JAR_NAME = "test-from-dependency.jar";
  private static final String TEST_TO_DEPENDENCY_JAR_NAME = "test-to-dependency.jar";

  private static final String[] FROM_CONNECTOR_SOURCE_FILES = {
    "TestConnectorClasspathIsolation/from/TestFromConnector.java",
    "TestConnectorClasspathIsolation/from/TestExtractor.java",
    "TestConnectorClasspathIsolation/from/TestFromDestroyer.java",
    "TestConnectorClasspathIsolation/from/TestFromInitializer.java",
    "TestConnectorClasspathIsolation/from/TestFromJobConfiguration.java",
    "TestConnectorClasspathIsolation/from/TestPartition.java",
    "TestConnectorClasspathIsolation/from/TestPartitioner.java",
    "TestConnectorClasspathIsolation/from/TestFromLinkConfiguration.java"
  };

  private static final String[] FROM_CONNECTOR_DEPENDENCY_SOURCE_FILES = {
    "TestConnectorClasspathIsolation/from/TestClasspathIsolation.java"
  };

  private static final String[] FROM_CONNECTOR_PROPERTY_FILES = {
    "TestConnectorClasspathIsolation/from/sqoopconnector.properties"
  };

  private static final String[] TO_CONNECTOR_SOURCE_FILES = {
    "TestConnectorClasspathIsolation/to/TestToConnector.java",
    "TestConnectorClasspathIsolation/to/TestLoader.java",
    "TestConnectorClasspathIsolation/to/TestToDestroyer.java",
    "TestConnectorClasspathIsolation/to/TestToInitializer.java",
    "TestConnectorClasspathIsolation/to/TestToJobConfiguration.java",
    "TestConnectorClasspathIsolation/to/TestToLinkConfiguration.java"
  };

  private static final String[] TO_CONNECTOR_DEPENDENCY_SOURCE_FILES = {
    "TestConnectorClasspathIsolation/to/TestClasspathIsolation.java"
  };

  private static final String[] TO_CONNECTOR_PROPERTY_FILES = {
    "TestConnectorClasspathIsolation/to/sqoopconnector.properties"
  };

  private ClassLoader classLoader;
  private SqoopMiniCluster sqoopMiniCluster;

  public static class DerbySqoopMiniCluster extends JettySqoopMiniCluster {

    private String extraClasspath;

    public DerbySqoopMiniCluster(String temporaryPath, Configuration configuration, String extraClasspath) throws Exception {
      super(temporaryPath, configuration);
      this.extraClasspath = extraClasspath;
    }

    @Override
    protected Map<String, String> getClasspathConfiguration() {
      Map<String, String> properties = new HashMap<>();

      if (extraClasspath != null) {
        properties.put(ConfigurationConstants.CLASSPATH, extraClasspath);
      }

      return properties;
    }
  }

  public void startSqoopMiniCluster(String extraClasspath) throws Exception {
    // And use them for new Derby repo instance
    sqoopMiniCluster = new DerbySqoopMiniCluster(HdfsUtils.joinPathFragments(super.getTemporaryPath(), getTestName()), getHadoopConf(), extraClasspath);
    KdcInfrastructureProvider kdcProvider = getInfrastructureProvider(KdcInfrastructureProvider.class);
    if (kdcProvider != null) {
      sqoopMiniCluster.setKdc(kdcProvider.getInstance());
    }

    // Start server
    sqoopMiniCluster.start();

    // Initialize Sqoop Client API
    initSqoopClient(sqoopMiniCluster.getServerUrl());
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
  public void testConnectorClasspathIsolation() throws Exception {
    Map<String, String> fromConnectorJarMap = ConnectorUtils.compileTestConnectorAndDependency(
        FROM_CONNECTOR_SOURCE_FILES,
        FROM_CONNECTOR_DEPENDENCY_SOURCE_FILES,
        FROM_CONNECTOR_PROPERTY_FILES,
        TEST_FROM_CONNECTOR_JAR_NAME,
        TEST_FROM_DEPENDENCY_JAR_NAME,
        true);
    Map<String, String> toConnectorJarMap = ConnectorUtils.compileTestConnectorAndDependency(
        TO_CONNECTOR_SOURCE_FILES,
        TO_CONNECTOR_DEPENDENCY_SOURCE_FILES,
        TO_CONNECTOR_PROPERTY_FILES,
        TEST_TO_CONNECTOR_JAR_NAME,
        TEST_TO_DEPENDENCY_JAR_NAME,
        true);
    startSqoopMiniCluster(
        StringUtils.join(Arrays.asList(fromConnectorJarMap.get(TEST_FROM_CONNECTOR_JAR_NAME), toConnectorJarMap.get(TEST_TO_CONNECTOR_JAR_NAME)), ":"));

    MJob job = prepareJob();

    prepareDriverConfig(job);

    saveJob(job);

    executeJob(job);

    stopSqoop();
    ConnectorUtils.deleteJars(fromConnectorJarMap);
  }

  private MJob prepareJob() {
    MLink rdbmsConnection = getClient().createLink("test-from-connector");
    saveLink(rdbmsConnection);

    MLink testConnection = getClient().createLink("test-to-connector");
    saveLink(testConnection);

    MJob job = getClient().createJob(rdbmsConnection.getName(), testConnection.getName());

    return job;
  }

  private MDriverConfig prepareDriverConfig(MJob job) {
    MDriverConfig driverConfig = job.getDriverConfig();
    driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

    return driverConfig;
  }

  private void stopSqoop() throws Exception {
    sqoopMiniCluster.stop();
  }
}

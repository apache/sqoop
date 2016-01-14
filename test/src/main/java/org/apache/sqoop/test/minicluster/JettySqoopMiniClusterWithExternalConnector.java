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
package org.apache.sqoop.test.minicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.test.utils.ConnectorUtils;

import java.util.HashMap;
import java.util.Map;

public class JettySqoopMiniClusterWithExternalConnector extends JettySqoopMiniCluster {

  private String extraClasspath;

  /** {@inheritDoc} */
  public JettySqoopMiniClusterWithExternalConnector(String temporaryPath,
      Configuration configuration) throws Exception {
    super(temporaryPath, configuration);
    prepareConnector();
  }

  private void prepareConnector() throws Exception {
    String[] connectorSourceFiles = {
            "TestConnectorForShell/TestConnectorForShell.java",
            "TestConnectorForShell/TestExtractorForShell.java",
            "TestConnectorForShell/TestFromDestroyerForShell.java",
            "TestConnectorForShell/TestFromInitializerForShell.java",
            "TestConnectorForShell/TestFromJobConfigForShell.java",
            "TestConnectorForShell/TestFromJobConfigurationForShell.java",
            "TestConnectorForShell/TestLinkConfigForShell.java",
            "TestConnectorForShell/TestLinkConfigurationForShell.java",
            "TestConnectorForShell/TestLoaderForShell.java",
            "TestConnectorForShell/TestPartitionerForShell.java",
            "TestConnectorForShell/TestPartitionForShell.java",
            "TestConnectorForShell/TestToDestroyerForShell.java",
            "TestConnectorForShell/TestToInitializerForShell.java",
            "TestConnectorForShell/TestToJobConfigForShell.java",
            "TestConnectorForShell/TestToJobConfigurationForShell.java"
    };
    String[] connectorPropertyFiles = {
            "TestConnectorForShell/sqoopconnector.properties",
            "TestConnectorForShell/test-connector-for-shell.properties"
    };

    String testConnectorJarName = "test-connector-for-shell.jar";

    Map<String, String> connectorJarMap = ConnectorUtils.compileTestConnectorAndDependency(
            connectorSourceFiles,
            new String[]{},
            connectorPropertyFiles,
            testConnectorJarName,
            "", false);
    extraClasspath = connectorJarMap.get(testConnectorJarName);
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

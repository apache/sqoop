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

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.test.infrastructure.Infrastructure;
import org.apache.sqoop.test.infrastructure.SqoopTestCase;
import org.apache.sqoop.test.infrastructure.providers.HadoopInfrastructureProvider;
import org.apache.sqoop.test.infrastructure.providers.KdcInfrastructureProvider;
import org.apache.sqoop.test.minicluster.JettySqoopMiniCluster;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.HdfsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = "no-real-cluster")
@Infrastructure(dependencies = {KdcInfrastructureProvider.class, HadoopInfrastructureProvider.class})
public class BlacklistedConnectorTest extends SqoopTestCase {
  private SqoopMiniCluster sqoopMiniCluster;

  public static class DerbySqoopMiniCluster extends JettySqoopMiniCluster {
    public DerbySqoopMiniCluster(String temporaryPath, Configuration configuration) throws Exception {
      super(temporaryPath, configuration);
    }

    @Override
    protected Map<String, String> getBlacklistedConnectorConfiguration() {
      Map<String, String> properties = new HashMap<>();

      properties.put(ConfigurationConstants.BLACKLISTED_CONNECTORS, "fake-connector:generic-jdbc-connector");
      return properties;
    }
  }

  @BeforeMethod(dependsOnMethods = { "init" })
  public void startSqoopMiniCluster() throws Exception {
    // And use them for new Derby repo instance
    sqoopMiniCluster = new DerbySqoopMiniCluster(HdfsUtils.joinPathFragments(super
        .getTemporaryPath(), getTestName()), getHadoopConf());
    KdcInfrastructureProvider kdcProvider = getInfrastructureProvider(KdcInfrastructureProvider.class);
    if (kdcProvider != null) {
      sqoopMiniCluster.setKdc(kdcProvider.getInstance());
    }

    // Start server
    sqoopMiniCluster.start();

    // Initialize Sqoop Client API
    initSqoopClient(sqoopMiniCluster.getServerUrl());
  }

  @Test(expectedExceptions = {SqoopException.class})
  public void testCreateLinkWithNonexistantConnector() throws Exception {
    getClient().createLink("generic-jdbc-connector");
  }

  @AfterMethod
  public void stopCluster() throws Exception {
    sqoopMiniCluster.stop();
  }
}

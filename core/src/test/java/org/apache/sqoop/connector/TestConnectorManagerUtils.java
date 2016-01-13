/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.sqoop.core.ConfigurationConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class TestConnectorManagerUtils {

  private String workingDir;
  private String testConnectorPath;
  private String testNonConnectorPath;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    workingDir = System.getProperty("user.dir");
    testConnectorPath =  workingDir + "/src/test/resources/test-connector.jar";
    testNonConnectorPath = workingDir + "/src/test/resources/test-non-connector.jar";
  }

  @Test
  public void testIsConnectorJar() {
    File connectorJar = new File(testConnectorPath);
    assertTrue(connectorJar.exists());
    assertTrue(ConnectorManagerUtils.isConnectorJar(connectorJar));
  }

  @Test
  public void testIsNotConnectorJar() {
    File file = new File(testNonConnectorPath);
    assertTrue(file.exists());
    assertFalse(ConnectorManagerUtils.isConnectorJar(file));
  }

  @Test
  public void testIsBlacklisted() throws Exception {
    URL url = new URL("jar:file:" +  testConnectorPath + "!/" + ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);

    Set<String> blacklistedConnectors = new HashSet<>();
    // The test connector is a copy of the generic jdbc connector
    blacklistedConnectors.add("generic-jdbc-connector");
    assertTrue(ConnectorManagerUtils.isBlacklisted(url, blacklistedConnectors));

    blacklistedConnectors = new HashSet<>();
    blacklistedConnectors.add("not-there");
    assertFalse(ConnectorManagerUtils.isBlacklisted(url, blacklistedConnectors));
  }

}

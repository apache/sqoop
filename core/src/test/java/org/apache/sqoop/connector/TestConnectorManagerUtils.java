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

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import org.apache.sqoop.utils.ClassUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Set;

public class TestConnectorManagerUtils {

  private String workingDir;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    workingDir = System.getProperty("user.dir");
  }

  @Test
  public void testGetConnectorJarsNullPath() {
    Set<File> files = ConnectorManagerUtils.getConnectorJars(null);
    assertNull(files);
  }

  @Test
  public void testGetConnectorJarsNonNullPath() {
    String path = workingDir + "/src/test/resources";
    Set<File> files = ConnectorManagerUtils.getConnectorJars(path);
    assertEquals(1, files.size());
  }

  @Test
  public void testIsConnectorJar() {
    String path = workingDir + "/src/test/resources/test-connector.jar";
    File connectorJar = new File(path);
    assertTrue(connectorJar.exists());
    assertTrue(ConnectorManagerUtils.isConnectorJar(connectorJar));
  }

  @Test
  public void testIsNotConnectorJar() {
    String path = workingDir + "/src/test/resources/test-non-connector.jar";
    File file = new File(path);
    assertTrue(file.exists());
    assertFalse(ConnectorManagerUtils.isConnectorJar(file));
  }

  @Test
  public void testAddExternalConnectorJarToClasspath() {
    String path = workingDir + "/src/test/resources";
    ConnectorManagerUtils.addExternalConnectorsJarsToClasspath(path);
    List<URL> urls = ConnectorManagerUtils.getConnectorConfigs();
    assertEquals(1, urls.size());
    ClassUtils.loadClass("org.apache.sqoop.connector.jdbc.GenericJdbcConnector");
  }

}

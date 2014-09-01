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
package org.apache.sqoop.connector;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Utilities for ConnectorManager.
 */
public class ConnectorManagerUtils {

  /**
   * Get a list of URLs of connectors that are installed.
   * Check
   * @return List of URLs.
   */
  public static List<URL> getConnectorConfigs() {
    List<URL> connectorConfigs = new ArrayList<URL>();

    try {
      // Check ConnectorManager classloader.
      Enumeration<URL> appPathConfigs =
          ConnectorManager.class.getClassLoader().getResources(
              ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);
      while (appPathConfigs.hasMoreElements()) {
        connectorConfigs.add(appPathConfigs.nextElement());
      }

      // Check thread context classloader.
      ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
      if (ctxLoader != null) {
        Enumeration<URL> ctxPathConfigs = ctxLoader.getResources(
            ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);

        while (ctxPathConfigs.hasMoreElements()) {
          URL configUrl = ctxPathConfigs.nextElement();
          if (!connectorConfigs.contains(configUrl)) {
            connectorConfigs.add(configUrl);
          }
        }
      }
    } catch (IOException ex) {
      throw new SqoopException(ConnectorError.CONN_0001, ex);
    }

    return connectorConfigs;
  }
}

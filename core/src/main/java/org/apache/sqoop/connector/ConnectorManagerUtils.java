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

import org.apache.sqoop.classloader.ConnectorClassLoader;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.error.code.ConnectorError;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Utilities for ConnectorManager.
 */
public class ConnectorManagerUtils {

  /**
   * Get a list of URLs of connectors that are installed.
   *
   * @return List of URLs.
   */
  public static List<URL> getConnectorConfigs(Set<String> connectorBlacklist) {
    List<URL> connectorConfigs = new ArrayList<>();

    try {
      // Check ConnectorManager classloader.
      Enumeration<URL> appPathConfigs = ConnectorManager.class.getClassLoader().getResources(
          ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);
      while (appPathConfigs.hasMoreElements()) {
        URL connectorConfig = appPathConfigs.nextElement();
        if (!isBlacklisted(connectorConfig, connectorBlacklist)) {
          connectorConfigs.add(connectorConfig);
        }
      }

      // Check thread context classloader.
      ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
      if (ctxLoader != null) {
        Enumeration<URL> ctxPathConfigs = ctxLoader.getResources(ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);

        while (ctxPathConfigs.hasMoreElements()) {
          URL configUrl = ctxPathConfigs.nextElement();
          if (!connectorConfigs.contains(configUrl) && !isBlacklisted(configUrl, connectorBlacklist)) {
            connectorConfigs.add(configUrl);
          }
        }
      }
    } catch (IOException ex) {
      throw new SqoopException(ConnectorError.CONN_0001, ex);
    }

    return connectorConfigs;
  }

  static boolean isBlacklisted(URL connectorConfig, Set<String> connectorBlacklist) throws IOException {
    Properties properties = new Properties();
    properties.load(connectorConfig.openStream());
    String connectorName = properties.getProperty(ConfigurationConstants.CONNPROP_CONNECTOR_NAME);
    if (connectorName == null) {
      throw new IOException("malformed connector properties: " + connectorConfig.getPath());
    } else {
      return connectorBlacklist.contains(connectorName);
    }
  }

  static boolean isConnectorJar(File file) {
    try (JarFile jarFile = new JarFile(file)) {
      @SuppressWarnings("resource")
      JarEntry entry = jarFile.getJarEntry(ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);
      return entry != null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getConnectorJarPath(URL configFileUrl) {
    return configFileUrl.getPath().substring("file:".length(),
        configFileUrl.getPath().length() -
        ("!/" + ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES).length());
  }

  public static ClassLoader createConnectorClassLoader(
      final String connectorJarPath, final List<String> systemClasses) {
    try {
      return AccessController.doPrivileged(
        new PrivilegedExceptionAction<ClassLoader>() {
          @Override
          public ClassLoader run() throws IOException {
            return new ConnectorClassLoader(connectorJarPath,
                Thread.currentThread().getContextClassLoader(),
                systemClasses, false);
          }
        });
    } catch (PrivilegedActionException e) {
      throw new SqoopException(ConnectorError.CONN_0011, e);
    }
  }
}
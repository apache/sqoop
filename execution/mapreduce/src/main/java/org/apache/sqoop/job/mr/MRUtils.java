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

package org.apache.sqoop.job.mr;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManagerUtils;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.job.MRJobConstants;

public final class MRUtils {
  private static ClassLoader fromConnectorClassLoader;
  private static ClassLoader toConnectorClassLoader;

  private static boolean connectorClassLoaderInited = false;

  public static synchronized void initConnectorClassLoaders(Configuration conf) {
    if (connectorClassLoaderInited) {
      return;
    }

    // Create ConnectorClassLoader for from connector
    String partitionClass = conf.get(MRJobConstants.JOB_ETL_PARTITION);
    String fromConnectorName = conf.get(MRJobConstants.JOB_CONNECTOR_FROM_NAME);
    if (!StringUtils.isBlank(fromConnectorName)) {
      fromConnectorClassLoader = ConnectorManagerUtils.createConnectorClassLoader(
          getConnectorJarName(fromConnectorName), Arrays.asList(partitionClass));
    }

    // Create ConnectorClassLoader for to connector
    String toConnectorName = conf.get(MRJobConstants.JOB_CONNECTOR_TO_NAME);
    if (!StringUtils.isBlank(toConnectorName)) {
      toConnectorClassLoader = ConnectorManagerUtils.createConnectorClassLoader(
          getConnectorJarName(toConnectorName), null);
    }

    connectorClassLoaderInited = true;
  }

  public static ClassLoader getConnectorClassLoader(Direction direction) {
    switch (direction) {
      case FROM:
        return fromConnectorClassLoader;
      case TO:
        return toConnectorClassLoader;
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  private static String getConnectorJarName(String connectorName) {
    List<URL> configFileUrls = ConnectorManagerUtils.getConnectorConfigs(Collections.EMPTY_SET);
    try {
      for (URL configFileUrl : configFileUrls) {
        Properties properties = new Properties();
        properties.load(configFileUrl.openStream());
        if (connectorName.equals(properties.getProperty(ConfigurationConstants.CONNPROP_CONNECTOR_NAME))) {
          String connectorJarPath = ConnectorManagerUtils.getConnectorJarPath(configFileUrl);
          return connectorJarPath.substring(connectorJarPath.lastIndexOf("/") + 1);
        }
      }
    } catch (IOException e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0026, connectorName, e);
    }

    throw new SqoopException(MRExecutionError.MAPRED_EXEC_0026, connectorName);
  }

  public static synchronized void destroy() {
    connectorClassLoaderInited = false;
  }
}

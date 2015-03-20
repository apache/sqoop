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

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.error.code.ConnectorError;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.utils.ClassUtils;

public final class ConnectorHandler {

  private static final Logger LOG = Logger.getLogger(ConnectorHandler.class);

  private final Properties properties = new Properties();

  private final String connectorUrl;
  private final String connectorClassName;
  private final String connectorUniqueName;
  private final SqoopConnector connector;

  private MConnector connectorConfigurable;

  public ConnectorHandler(URL configFileUrl) {
    connectorUrl = configFileUrl.toString();
    try {
      properties.load(configFileUrl.openStream());
    } catch (IOException ex) {
      throw new SqoopException(ConnectorError.CONN_0003, configFileUrl.toString(), ex);
    }

    LOG.debug("Connector configuration: " + properties);

    connectorClassName = properties.getProperty(
        ConfigurationConstants.CONPROP_PROVIDER_CLASS);

    if (connectorClassName == null || connectorClassName.trim().length() == 0) {
      throw new SqoopException(ConnectorError.CONN_0004,
          ConfigurationConstants.CONPROP_PROVIDER_CLASS);
    }

    connectorUniqueName = properties.getProperty(ConfigurationConstants.CONNPROP_CONNECTOR_NAME);

    if (connectorUniqueName == null || connectorUniqueName.trim().length() == 0) {
      throw new SqoopException(ConnectorError.CONN_0008, connectorClassName);
    }

    Class<?> connectorClass = ClassUtils.loadClass(connectorClassName);
    if(connectorClass == null) {
      throw new SqoopException(ConnectorError.CONN_0005, connectorClassName);
    }

    try {
      connector = (SqoopConnector) connectorClass.newInstance();
    } catch (IllegalAccessException ex) {
      throw new SqoopException(ConnectorError.CONN_0005,
              connectorClassName, ex);
    } catch (InstantiationException ex) {
      throw new SqoopException(ConnectorError.CONN_0005,
          connectorClassName, ex);
    }

    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (connector.getSupportedDirections().contains(Direction.FROM)) {
      fromConfig = new MFromConfig(ConfigUtils.toConfigs(
          connector.getJobConfigurationClass(Direction.FROM)));
    }

    if (connector.getSupportedDirections().contains(Direction.TO)) {
      toConfig = new MToConfig(ConfigUtils.toConfigs(
          connector.getJobConfigurationClass(Direction.TO)));
    }

    MLinkConfig linkConfig = new MLinkConfig(
        ConfigUtils.toConfigs(connector.getLinkConfigurationClass()));

    connectorConfigurable = new MConnector(connectorUniqueName, connectorClassName, connector.getVersion(),
        linkConfig, fromConfig, toConfig);

    if (LOG.isInfoEnabled()) {
      LOG.info("Connector [" + connectorClassName + "] initialized.");
    }
  }

  public String toString() {
    return "{" + connectorUniqueName + ":" + connectorClassName
        + ":" + connectorUrl + "}";
  }

  public String getUniqueName() {
    return connectorUniqueName;
  }

  public String getConnectorClassName() {
    return connectorClassName;
  }

  public String getConnectorUrl() {
    return connectorUrl;
  }

  public MConnector getConnectorConfigurable() {
    return connectorConfigurable;
  }

  public void setConnectorConfigurable(MConnector mConnector) {
    this.connectorConfigurable = mConnector;
  }

  public SqoopConnector getSqoopConnector() {
    return connector;
  }
}

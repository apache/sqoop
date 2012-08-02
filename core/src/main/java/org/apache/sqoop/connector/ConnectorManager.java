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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.repository.RepositoryTransaction;
import org.apache.sqoop.model.MConnector;

public class ConnectorManager {

  private static final Logger LOG = Logger.getLogger(ConnectorManager.class);

  // key: connector id, value: connector name
  private static Map<Long, String> nameMap = new HashMap<Long, String>();

  // key: connector name, value: connector handler
  private static Map<String, ConnectorHandler> handlerMap =
      new HashMap<String, ConnectorHandler>();

  public static MConnector[] getConnectors() {
    MConnector[] connectors = new MConnector[handlerMap.size()];
    int indx = 0;
    for (ConnectorHandler handler : handlerMap.values()) {
      connectors[indx++] = handler.getMetadata();
    }
    return connectors;
  }

  public static MConnector getConnector(long connectorId) {
    ConnectorHandler handler = handlerMap.get(nameMap.get(connectorId));
    return handler.getMetadata();
  }

  public static synchronized void initialize() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Begin connector manager initialization");
    }

    List<URL> connectorConfigs = new ArrayList<URL>();

    try {
      Enumeration<URL> appPathConfigs =
          ConnectorManager.class.getClassLoader().getResources(
              ConfigurationConstants.FILENAME_CONNECTOR_PROPERTIES);

      while (appPathConfigs.hasMoreElements()) {
        connectorConfigs.add(appPathConfigs.nextElement());
      }

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

      LOG.info("Connector config urls: " + connectorConfigs);

      if (connectorConfigs.size() == 0) {
        throw new SqoopException(ConnectorError.CONN_0002);
      }

      for (URL url : connectorConfigs) {
        ConnectorHandler handler = new ConnectorHandler(url);
        ConnectorHandler handlerOld =
            handlerMap.put(handler.getUniqueName(), handler);
        if (handlerOld != null) {
          throw new SqoopException(ConnectorError.CONN_0006,
              handler + ", " + handlerOld);
        }
      }
    } catch (IOException ex) {
      throw new SqoopException(ConnectorError.CONN_0001, ex);
    }

    registerConnectors();

    if (LOG.isInfoEnabled()) {
      LOG.info("Connectors loaded: " + handlerMap);
    }
  }

  private static synchronized void registerConnectors() {
    Repository repository = RepositoryManager.getRepository();

    RepositoryTransaction rtx = null;
    try {
      rtx = repository.getTransaction();
      rtx.begin();
      for (String name : handlerMap.keySet()) {
        ConnectorHandler handler = handlerMap.get(name);
        MConnector connectorMetadata = handler.getMetadata();
        MConnector registeredMetadata =
            repository.registerConnector(connectorMetadata);
        if (registeredMetadata != null) {
          // Verify that the connector metadata is the same
          if (!registeredMetadata.equals(connectorMetadata)) {
            throw new SqoopException(ConnectorError.CONN_0009,
                "To register: " + connectorMetadata + "; already registered: "
                + registeredMetadata);
          }
        }

        String connectorName = handler.getUniqueName();
        if (!handler.getMetadata().hasPersistenceId()) {
          throw new SqoopException(ConnectorError.CONN_0010, connectorName);
        }
        nameMap.put(handler.getMetadata().getPersistenceId(), connectorName);
      }
      rtx.commit();
    } catch (Exception ex) {
      if (rtx != null) {
        rtx.rollback();
      }
      throw new SqoopException(ConnectorError.CONN_0007, ex);
    } finally {
      if (rtx != null) {
        rtx.close();
      }
    }
  }


  public static synchronized void destroy() {
    // FIXME
  }

}

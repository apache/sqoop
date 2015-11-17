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
package org.apache.sqoop.handler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.ConnectorsBean;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.security.authorization.AuthorizationEngine;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;

public class ConnectorRequestHandler implements RequestHandler {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = Logger.getLogger(ConnectorRequestHandler.class);

  public ConnectorRequestHandler() {
    LOG.info("ConnectorRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    List<MConnector> connectors;
    Map<Long, ResourceBundle> configParamBundles;
    Locale locale = ctx.getAcceptLanguageHeader();
    String cIdentifier = ctx.getLastURLElement();

    LOG.info("ConnectorRequestHandler handles cid: " + cIdentifier);

    if (cIdentifier.equals("all")) {
      connectors = ConnectorManager.getInstance().getConnectorConfigurables();
      configParamBundles = ConnectorManager.getInstance().getResourceBundles(locale);
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "connectors", "all");

      // Authorization check
      connectors = AuthorizationEngine.filterResource(ctx.getUserName(), MResource.TYPE.CONNECTOR, connectors);

      return new ConnectorsBean(connectors, configParamBundles);

    } else {
      // NOTE: we now support using unique name as well as the connector id
      // NOTE: connectorId is a fallback for older sqoop clients if any, since we want to primarily use unique conenctorNames
      MConnector mConnector = HandlerUtils.getConnectorFromConnectorName(cIdentifier);

      configParamBundles = new HashMap<>();

      MConnector connector = ConnectorManager.getInstance().getConnectorConfigurable(mConnector.getUniqueName());
      configParamBundles.put(connector.getPersistenceId(),
          ConnectorManager.getInstance().getResourceBundle(mConnector.getUniqueName(), locale));

      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "connector", mConnector.getUniqueName());

      // Authorization check
      AuthorizationEngine.readConnector(ctx.getUserName(), connector.getUniqueName());

      return new ConnectorBean(Arrays.asList(connector), configParamBundles);
    }
  }
}
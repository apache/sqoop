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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Connector request handler is supporting following resources:
 *
 * GET /v1/connector/:cid
 * Return details about one particular connector with id :cid or about all of
 * them if :cid equals to "all".
 *
 * Planned resources:
 *
 * GET /v1/connector
 * Get brief list of all connectors present in the system. This resource is not
 * yet implemented.
 *
 */
public class ConnectorRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectorRequestHandler.class);

  public ConnectorRequestHandler() {
    LOG.info("ConnectorRequestHandler initialized");
  }


  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    List<MConnector> connectors;
    Map<Long, ResourceBundle> bundles;
    Locale locale = ctx.getAcceptLanguageHeader();

    String cid = ctx.getLastURLElement();

    LOG.info("ConnectorRequestHandler handles cid: " + cid);
    if (cid.equals("all")) {
      // display all connectors
      connectors = ConnectorManager.getInstance().getConnectorsMetadata();
      bundles = ConnectorManager.getInstance().getResourceBundles(locale);
    } else {
      Long id = Long.parseLong(cid);

      // Check that user is not asking for non existing connector id
      if(!ConnectorManager.getInstance().getConnectorIds().contains(id)) {
        throw new SqoopException(ServerError.SERVER_0004, "Invalid id " + id);
      }

      connectors = new LinkedList<MConnector>();
      bundles = new HashMap<Long, ResourceBundle>();

      connectors.add(ConnectorManager.getInstance().getConnectorMetadata(id));
      bundles.put(id, ConnectorManager.getInstance().getResourceBundle(id, locale));
    }

    return new ConnectorBean(connectors, bundles);
  }
}

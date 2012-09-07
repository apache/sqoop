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

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public class ConnectorRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectorRequestHandler.class);

  public ConnectorRequestHandler() {
    LOG.info("ConnectorRequestHandler initialized");
  }


  @Override
  public JsonBean handleEvent(RequestContext ctx) throws SqoopException {
    List<MConnector> connectors;
    List<ResourceBundle> bundles;
    Locale locale = ctx.getAcceptLanguageHeader();

    String cid = ctx.getLastURLElement();

    LOG.info("ConnectorRequestHandler handles cid: " + cid);
    if (cid.equals("all")) {
      // display all connectors
      connectors = ConnectorManager.getConnectorsMetadata();
      bundles = ConnectorManager.getResourceBundles(locale);

    } else {
      Long id = Long.parseLong(cid);

      connectors = new LinkedList<MConnector>();
      bundles = new LinkedList<ResourceBundle>();

      connectors.add(ConnectorManager.getConnectorMetadata(id));
      bundles.add(ConnectorManager.getResourceBundle(id, locale));
    }

    return new ConnectorBean(connectors, bundles);
  }
}

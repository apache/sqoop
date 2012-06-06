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
import org.apache.sqoop.connector.ConnectorHandler;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;

public class ConnectorRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectorRequestHandler.class);

  /** The API version supported by this server */
  public static final String PROTOCOL_V1 = "1";


  private ConnectorBean connectors;

  public ConnectorRequestHandler() {
    LOG.info("ConnectorRequestHandler initialized");
  }


  @Override
  public JsonBean handleEvent(RequestContext ctx) throws SqoopException {
    ConnectorBean connectorBean;

    String uri = ctx.getRequest().getRequestURI();
    int slash = uri.lastIndexOf("/");
    String cid = uri.substring(slash + 1);
    LOG.info("ConnectorRequestHandler handles cid: " + cid);
    if (cid.equals("all")) {
      // display all connectors
      if (connectors == null) {
        ConnectorHandler[] handlers = ConnectorManager.getHandlers();
        long[] ids = new long[handlers.length];
        String[] names = new String[handlers.length];
        String[] classes = new String[handlers.length];
        for (int i = 0; i < handlers.length; i++) {
          ids[i] = handlers[i].getMetadata().getPersistenceId();
          names[i] = handlers[i].getUniqueName();
          classes[i] = handlers[i].getConnectorClassName();
        }
        connectors = new ConnectorBean(ids, names, classes);
      }
      connectorBean = connectors;

    } else {
      // display one connector
      ConnectorHandler handler =
          ConnectorManager.getHandler(Long.parseLong(cid));
      long[] ids = new long[] { handler.getMetadata().getPersistenceId() };
      String[] names = new String[] { handler.getUniqueName() };
      String[] classes = new String[] { handler.getConnectorClassName() };
      connectorBean = new ConnectorBean(ids, names, classes);
    }

    return connectorBean;
  }
}

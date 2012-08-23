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
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;

public class ConnectorRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectorRequestHandler.class);

  public ConnectorRequestHandler() {
    LOG.info("ConnectorRequestHandler initialized");
  }


  @Override
  public JsonBean handleEvent(RequestContext ctx) throws SqoopException {
    MConnector[] connectors;

    String uri = ctx.getRequest().getRequestURI();
    int slash = uri.lastIndexOf("/");
    String cid = uri.substring(slash + 1);
    LOG.info("ConnectorRequestHandler handles cid: " + cid);
    if (cid.equals("all")) {
      // display all connectors
      connectors = ConnectorManager.getConnectors();

    } else {
      // display one connector
      connectors = new MConnector[] {
          ConnectorManager.getConnector(Long.parseLong(cid)) };
    }

    return new ConnectorBean(connectors);
  }
}

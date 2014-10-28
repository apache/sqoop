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
package org.apache.sqoop.server.v1;

import org.apache.sqoop.handler.ConnectorRequestHandler;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.SqoopProtocolServlet;


/**
 * Displays connector(s) registered in sqoop
 *
 * GET v1/connector/all (remains for backward compatibility)
 *  Return all connectors registered in the sqoop system with their corresponding config params
 * GET /v1/connector/{cname}
 *  Return details about one particular connector with name {cname} with its config params
 * GET /v1/connector/{cid}
 *  Return details about one particular connector with id {cid} with its config params
 *
 */
@SuppressWarnings("serial")
public class ConnectorServlet extends SqoopProtocolServlet {

  private RequestHandler connectorRequestHandler;

  public ConnectorServlet() {
    connectorRequestHandler = new ConnectorRequestHandler();
  }

  @Override
  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    return connectorRequestHandler.handleEvent(ctx);
  }
}

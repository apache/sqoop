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
import org.apache.sqoop.handler.DriverRequestHandler;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.SqoopProtocolServlet;

/**
 * Displays a given configurable registered in sqoop
 * GET v1/configurable/connector/{cname}
 *  Return a registered connector with  given name
 * GET v1/configurable/connector/{cid}
 *  Return a registered connector with given id
 * GET v1/configurable/driver
 *  Return the only driver registered in sqoop
 */
@SuppressWarnings("serial")
public class ConfigurableServlet extends SqoopProtocolServlet {

  private RequestHandler configurableRequestHandler;
  private static String CONNECTOR_CONFIGURABLE = "connector";
  private static String DRIVER_CONFIGURABLE = "connector";

  public ConfigurableServlet() {
    // be default
    configurableRequestHandler = new DriverRequestHandler();
  }

  @Override
  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    if (ctx.getPath().contains(CONNECTOR_CONFIGURABLE)) {
      configurableRequestHandler = new ConnectorRequestHandler();
    } else if (ctx.getPath().contains(DRIVER_CONFIGURABLE)) {
      configurableRequestHandler = new DriverRequestHandler();
    }
    return configurableRequestHandler.handleEvent(ctx);
  }
}

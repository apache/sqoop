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

import org.apache.sqoop.handler.LinkRequestHandler;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.SqoopProtocolServlet;

/**
 * Provides operations for link resource
 *
 * GET /v1/link/{lid}
 *  Return details about one particular link with id lid
 * GET /v1/link/{lname}
 *  Return details about one particular link with name lname
 *
 * POST /v1/link/ with {connector-id} and {link-config-id} in the post data
 *  Create link for connector with id connector-id
 * PUT /v1/link/ with {connector-id} and {link-config-id} in the post data
 *  Edit/Update link for connector with id connector-id
 *
 * PUT /v1/link/{lid}
 *  Edit/Update details about one particular link with id lid
 * PUT /v1/link/{lname}
 *  Edit/Update details about one particular link with name lname
 *
 * DELETE /v1/link/{lid}
 *  Delete/Remove one particular link with id lid
 * DELETE /v1/link/{lname}
 *  Delete/Remove one particular link with name lname
 *
 * PUT /v1/link/{lname}/enable
 * Enable link with name lname
 *
 * PUT /v1/link/{lname}/disable
 * Disable link with name lname
 *
 */
@SuppressWarnings("serial")
public class LinkServlet extends SqoopProtocolServlet {

  private RequestHandler linkRequestHandler;

  public LinkServlet() {
    linkRequestHandler = new LinkRequestHandler();
  }

  @Override
  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    return linkRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePostRequest(RequestContext ctx) throws Exception {
    return linkRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePutRequest(RequestContext ctx) throws Exception {
    return linkRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handleDeleteRequest(RequestContext ctx) throws Exception {
    return linkRequestHandler.handleEvent(ctx);
  }
}
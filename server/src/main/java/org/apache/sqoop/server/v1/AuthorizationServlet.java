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

import org.apache.sqoop.handler.AuthorizationRequestHandler;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.SqoopProtocolServlet;

/**
 * Provides operations for authorization
 *
 * POST v1/authorization/roles/create
 * Create new role with {name}
 *
 * DELETE v1/authorization/role/{role-name}
 *
 * GET v1/authorization/roles
 * Show all roles
 *
 * GET v1/authorization/principals?role_name={name}
 * Show all principals in role with {name}
 *
 * GET v1/authorization/roles?principal_type={type}&principal_name={name}
 * Show all roles in principal with {name, type}
 *
 * PUT v1/authorization/roles/grant
 * Grant a role to a user/group/role
 * PUT data of JsonObject role(name) and principal (name, type)
 *
 * PUT v1/authorization/roles/revoke
 * Revoke a role to a user/group/role
 * PUT data of JsonObject role(name) and principal (name, type)
 *
 * PUT v1/authorization/privileges/grant
 * Grant a privilege to a principal
 * PUT data of JsonObject principal(name, type) and privilege (resource-name, resource-type, action, with-grant-option)
 *
 * PUT v1/authorization/privileges/revoke
 * Revoke a privilege to a principal
 * PUT data of JsonObject principal(name, type) and privilege (resource-name, resource-type, action, with-grant-option)
 * If privilege is null, then revoke all privileges for principal(name, type)
 *
 * GET v1/authorization/privileges?principal_type={type}&principal_name={name}&resource_type={type}&resource_name={name}
 * Show all privileges in principal with {name, type} and resource with {resource-name, resource-type}
 * If resource is null, then show all privileges in principal with {name, type}
 */
@SuppressWarnings("serial")
public class AuthorizationServlet extends SqoopProtocolServlet {

  private RequestHandler authorizationRequestHandler;

  public AuthorizationServlet() {
    authorizationRequestHandler = new AuthorizationRequestHandler();
  }

  @Override
  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    return authorizationRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePostRequest(RequestContext ctx) throws Exception {
    return authorizationRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePutRequest(RequestContext ctx) throws Exception {
    return authorizationRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handleDeleteRequest(RequestContext ctx) throws Exception {
    return authorizationRequestHandler.handleEvent(ctx);
  }
}

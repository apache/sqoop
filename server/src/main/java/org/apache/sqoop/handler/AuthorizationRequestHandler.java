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
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.json.*;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.AuthorizationHandler;
import org.apache.sqoop.security.AuthorizationManager;
import org.apache.sqoop.security.SecurityError;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.List;

public class AuthorizationRequestHandler implements RequestHandler {

  /**
   * enum for representing the actions supported on the authorization
   */
  enum Action {
    AUTHORIZATION("authorization"), //parent url path
    ROLES("roles"),                 //first level url path
    PRINCIPALS("principals"),       //first level url path
    PRIVILEGES("privileges"),       //first level url path
    CREATE("create"),               //second level url path
    GRANT("grant"),                 //second level url path
    REVOKE("revoke");               //second level url path

    Action(String name) {
      this.name = name;
    }

    String name;

    public static Action fromString(String name) {
      if (name != null) {
        for (Action action : Action.values()) {
          if (name.equalsIgnoreCase(action.name)) {
            return action;
          }
        }
      }
      return null;
    }
  }

  private static final Logger LOG = Logger.getLogger(AuthorizationRequestHandler.class);

  public AuthorizationRequestHandler() {
    LOG.info("AuthorizationRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    Action action = Action.fromString(ctx.getLastURLElement());
    String url = ctx.getRequest().getRequestURI();
    switch (ctx.getMethod()) {
      case GET:
        switch (action) {
          case ROLES:                                   //url: /authorization/roles
            return getRoles(ctx);
          case PRINCIPALS:                              //url: /authorization/principals
            return getPrincipal(ctx);
          case PRIVILEGES:                              //url: /authorization/privileges
            return getPrivilege(ctx);
          default:
            throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
        }
      case POST:
        switch (action) {
          case CREATE:                              //url: /authorization/roles/create
            return createRole(ctx);
          default:
            throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
        }
      case PUT:
        String[] urlElements = ctx.getUrlElements();
        Action first_level_action = Action.fromString(urlElements[urlElements.length - 2]);
        switch (first_level_action) {
          case ROLES:
            switch (action) {
              case GRANT:                               //url: /authorization/roles/grant
                return grantRevokeRole(ctx, true);
              case REVOKE:                              //url: /authorization/roles/revoke
                return grantRevokeRole(ctx, false);
              default:
                throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
            }
          case PRIVILEGES:
            switch (action) {
              case GRANT:                               //url: /authorization/privileges/grant
                return grantRevokePrivilege(ctx, true);
              case REVOKE:                              //url: /authorization/privileges/revoke
                return grantRevokePrivilege(ctx, false);
              default:
                throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
            }
          default:
            throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
        }
      case DELETE:                                      //url: /authorization/roles/{role_name}
        return dropRole(ctx);
      default:
        throw new SqoopException(ServerError.SERVER_0003, "Invalid action in url" + url);
    }
  }

  private JsonBean getRoles(RequestContext ctx) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();
    String principal_name = ctx.getParameterValue(PRINCIPAL_NAME_QUERY_PARAM);
    String principal_type = ctx.getParameterValue(PRINCIPAL_TYPE_QUERY_PARAM);

    if (principal_name != null && principal_type != null) {
      // get roles by principal
      MPrincipal principal = new MPrincipal(principal_name, principal_type);
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "get", "roles by principal", principal.toString());
      return new RolesBean(handler.getRolesByPrincipal(principal));
    } else {
      // get all roles in the system
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "get", "roles", "all");
      return new RolesBean(handler.getAllRoles());
    }
  }

  private JsonBean getPrincipal(RequestContext ctx) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();
    String role_name = ctx.getParameterValue(ROLE_NAME_QUERY_PARAM);

    if (role_name != null) {
      // get principal by role
      MRole role = new MRole(role_name);
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "get", "principals by role", role.toString());
      return new PrincipalsBean(handler.getPrincipalsByRole(role));
    } else {
      throw new SqoopException(SecurityError.AUTH_0012, "Can't get role name");
    }
  }

  private JsonBean getPrivilege(RequestContext ctx) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();
    String principal_name = ctx.getParameterValue(PRINCIPAL_NAME_QUERY_PARAM);
    String principal_type = ctx.getParameterValue(PRINCIPAL_TYPE_QUERY_PARAM);
    String resource_name = ctx.getParameterValue(RESOURCE_NAME_QUERY_PARAM);
    String resource_type = ctx.getParameterValue(RESOURCE_TYPE_QUERY_PARAM);

    if (principal_name != null && principal_type != null) {
      // get privilege by principal
      MPrincipal principal = new MPrincipal(principal_name, principal_type);
      MResource resource = null;
      if (resource_name != null && resource_type != null) {
        resource = new MResource(resource_name, resource_type);
      }
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "get", "privileges by principal", principal.toString());
      return new PrivilegesBean(handler.getPrivilegesByPrincipal(principal, resource));
    } else {
      throw new SqoopException(SecurityError.AUTH_0013, "Can't get principal");
    }
  }

  private JsonBean createRole(RequestContext ctx) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();

    RoleBean bean = new RoleBean();

    try {
      JSONObject json = JSONUtils.parse(ctx.getRequest().getReader());
      bean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003, "Can't read request content", e);
    }

    // Get role object
    List<MRole> roles = bean.getRoles();

    if (roles.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003, "Expected one role but got " + roles.size());
    }

    MRole postedRole = roles.get(0);

    manager.logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "create", "role", postedRole.toString());
    handler.createRole(postedRole);
    return JsonBean.EMPTY_BEAN;
  }

  private JsonBean grantRevokeRole(RequestContext ctx, boolean isGrant) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();

    RolesBean rolesBean = new RolesBean();
    PrincipalsBean principalsBean = new PrincipalsBean();

    try {
      JSONObject json = JSONUtils.parse(ctx.getRequest().getReader());
      rolesBean.restore(json);
      principalsBean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003, "Can't read request content", e);
    }

    // Get role object
    List<MRole> roles = rolesBean.getRoles();
    // Get principal object
    List<MPrincipal> principals = principalsBean.getPrincipals();

    if (isGrant) {
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "grant", "role", "principal");
      handler.grantRole(principals, roles);
    } else {
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "revoke", "role", "principal");
      handler.revokeRole(principals, roles);
    }
    return JsonBean.EMPTY_BEAN;
  }

  private JsonBean grantRevokePrivilege(RequestContext ctx, boolean isGrant) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();

    PrincipalsBean principalsBean = new PrincipalsBean();
    PrivilegesBean privilegesBean = new PrivilegesBean();

    try {
      JSONObject json = JSONUtils.parse(ctx.getRequest().getReader());
      principalsBean.restore(json);
      try {
        privilegesBean.restore(json);
      } catch (Exception e) {//Privilege is null, revoke all privileges from principal
        privilegesBean = null;
      }
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003, "Can't read request content", e);
    }

    // Get principal object
    List<MPrincipal> principals = principalsBean.getPrincipals();
    // Get privilege object
    List<MPrivilege> privileges = privilegesBean == null ? null : privilegesBean.getPrivileges();

    if (isGrant) {
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "grant", "role", "privilege");
      handler.grantPrivileges(principals, privileges);
    } else {
      manager.logAuditEvent(ctx.getUserName(),
              ctx.getRequest().getRemoteAddr(), "revoke", "role", "privilege");
      handler.revokePrivileges(principals, privileges);
    }
    return JsonBean.EMPTY_BEAN;
  }

  private JsonBean dropRole(RequestContext ctx) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    AuditLoggerManager manager = AuditLoggerManager.getInstance();

    String[] urlElements = ctx.getUrlElements();

    //wrong URL
    if (urlElements.length < 2 ||
            !urlElements[urlElements.length - 2].equals(Action.ROLES.name)) {
      throw new SqoopException(SecurityError.AUTH_0012, "Can't get role name");
    }

    String role_name = ctx.getLastURLElement();
    MRole role = new MRole(role_name);
    manager.logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "delete", "role", role.toString());
    handler.dropRole(role);
    return JsonBean.EMPTY_BEAN;
  }
}
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
package org.apache.sqoop.client.request;

import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.json.*;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * Provide CRUD semantics over RESTfull HTTP API for authorization. All operations are
 * normally supported.
 */
public class AuthorizationResourceRequest extends ResourceRequest {

  public static final String RESOURCE = "v1/authorization";

  public static final String ROLES = "/roles";
  public static final String PRINCIPALS = "/principals";
  public static final String PRIVILEGES = "/privileges";

  private static final String CREATE = "/create";
  private static final String GRANT = "/grant";
  private static final String REVOKE = "/revoke";

  public AuthorizationResourceRequest() {
    super();
  }

  public AuthorizationResourceRequest(DelegationTokenAuthenticatedURL.Token token) {
    super(token);
  }

  public RolesBean readRoles(String serverUrl) {
    String response = super.get(serverUrl + RESOURCE + ROLES);
    JSONObject jsonObject = JSONUtils.parse(response);
    RolesBean bean = new RolesBean();
    bean.restore(jsonObject);
    return bean;
  }

  public void createRole(String serverUrl, MRole role) {
    RoleBean roleBean = new RoleBean(role);
    // Extract all config inputs including sensitive inputs
    JSONObject roleJson = roleBean.extract(false);
    super.post(serverUrl + RESOURCE + ROLES + CREATE, roleJson.toJSONString());
  }

  public void dropRole(String serverUrl, MRole role) {
    super.delete(serverUrl + RESOURCE + ROLES + "/" + role.getName());
  }

  public void grantRevokeRole(String serverUrl, List<MRole> roles, List<MPrincipal> principals, boolean isGrant) {
    RolesBean rolesBean = new RolesBean(roles);
    PrincipalsBean principalsBean = new PrincipalsBean(principals);
    // Extract all config inputs including sensitive inputs
    JSONObject jsonObject = new JSONObject();
    jsonObject.putAll(rolesBean.extract(false));
    jsonObject.putAll(principalsBean.extract(false));
    if (isGrant) {
      super.put(serverUrl + RESOURCE + ROLES + GRANT, jsonObject.toJSONString());
    } else {
      super.put(serverUrl + RESOURCE + ROLES + REVOKE, jsonObject.toJSONString());
    }
  }

  public RolesBean readRolesByPrincipal(String serverUrl, MPrincipal principal) {
    String response = super.get(serverUrl + RESOURCE + ROLES
            + "?principal_name=" + principal.getName()
            + "&principal_type=" + principal.getType());
    JSONObject jsonObject = JSONUtils.parse(response);
    RolesBean bean = new RolesBean();
    bean.restore(jsonObject);
    return bean;
  }

  public PrincipalsBean readPrincipalsByRole(String serverUrl, MRole role) {
    String response = super.get(serverUrl + RESOURCE + PRINCIPALS
            + "?role_name=" + role.getName());
    JSONObject jsonObject = JSONUtils.parse(response);
    PrincipalsBean bean = new PrincipalsBean();
    bean.restore(jsonObject);
    return bean;
  }

  public void grantRevokePrivilege(String serverUrl, List<MPrincipal> principals, List<MPrivilege> privileges, boolean isGrant) {
    PrincipalsBean principalsBean = new PrincipalsBean(principals);
    // Extract all config inputs including sensitive inputs
    JSONObject jsonObject = new JSONObject();
    jsonObject.putAll(principalsBean.extract(false));

    if (privileges != null && privileges.size() != 0) {
      PrivilegesBean privilegesBean = new PrivilegesBean(privileges);
      jsonObject.putAll(privilegesBean.extract(false));
    }

    if (isGrant) {
      super.put(serverUrl + RESOURCE + PRIVILEGES + GRANT, jsonObject.toJSONString());
    } else {
      super.put(serverUrl + RESOURCE + PRIVILEGES + REVOKE, jsonObject.toJSONString());
    }
  }

  public PrivilegesBean readPrivilegesByPrincipal(String serverUrl, MPrincipal principal, MResource resource) {
    String url = serverUrl + RESOURCE + PRIVILEGES
            + "?principal_name=" + principal.getName()
            + "&principal_type=" + principal.getType();
    if (resource != null) {
      url += "&resource_name=" + resource.getName();
      url += "&resource_type=" + resource.getType();
    }
    String response = super.get(url);
    JSONObject jsonObject = JSONUtils.parse(response);
    PrivilegesBean bean = new PrivilegesBean();
    bean.restore(jsonObject);
    return bean;
  }
}

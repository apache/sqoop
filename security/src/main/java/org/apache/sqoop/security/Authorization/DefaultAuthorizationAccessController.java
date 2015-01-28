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
package org.apache.sqoop.security.Authorization;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.AuthorizationAccessController;

import java.security.Principal;
import java.util.List;

public class DefaultAuthorizationAccessController extends AuthorizationAccessController {

  private static final Logger LOG = Logger.getLogger(DefaultAuthorizationAccessController.class);

  /**
   * Role related function
   */
  public List<MRole> getAllRoles() throws SqoopException {
    LOG.debug("Get all roles in default authorization access controller: return null");
    return null;
  }

  public MRole getRole(String name) throws SqoopException {
    LOG.debug("Get role in default authorization access controller: return null");
    LOG.debug("name: " + name);
    return null;
  }

  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    LOG.debug("Get roles by principal in default authorization access controller: return null");
    LOG.debug("principal: " + principal.toString());
    return null;
  }

  public List<MRole> getRolesByPrivilege(MPrivilege privilege) throws SqoopException {
    LOG.debug("Get roles by privilege in default authorization access controller: return null");
    LOG.debug("privilege: " + privilege.toString());
    return null;
  }

  public void createRole(String name) throws SqoopException {
    LOG.debug("Create role in default authorization access controller: empty function");
    LOG.debug("name: " + name);
  }

  public void updateRole(String old_name, String new_name) throws SqoopException {
    LOG.debug("Update role in default authorization access controller: empty function");
    LOG.debug("old name: " + old_name + ", new name: " + new_name);
  }

  public void removeRole(String name) throws SqoopException {
    LOG.debug("Remove role in default authorization access controller: empty function");
    LOG.debug("name: " + name);
  }

  /**
   * Principal related function
   */
  public List<MPrincipal> getAllPrincipals() throws SqoopException {
    LOG.debug("Get all principals in default authorization access controller: return null");
    return null;
  }

  public List<MPrincipal> getPrincipalsByName(String name) throws SqoopException {
    LOG.debug("Get principals by name in default authorization access controller: return null");
    LOG.debug("name: " + name);
    return null;
  }

  public List<MPrincipal> getPrincipalsByType(String type) throws SqoopException {
    LOG.debug("Get principals by type in default authorization access controller: return null");
    LOG.debug("type: " + type);
    return null;
  }

  public MPrincipal getPrincipal(String name, String type) throws SqoopException {
    LOG.debug("Get principal in default authorization access controller: return null");
    LOG.debug("name: " + name + ", type: " + type);
    return null;
  }

  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    LOG.debug("Get principals by role in default authorization access controller: return null");
    LOG.debug("role: " + role.toString());
    return null;
  }

  public void createPrincipal(String name, String type) throws SqoopException {
    LOG.debug("Create principal in default authorization access controller: empty function");
    LOG.debug("name: " + name + ", type: " + type);
  }

  public void updatePrincipal(MPrincipal old_principal, MPrincipal new_principal) throws SqoopException {
    LOG.debug("Update principal in default authorization access controller: empty function");
    LOG.debug("old principal: " + old_principal + ", new principal: " + new_principal);
  }

  public void removePrincipalsByName(String name) throws SqoopException {
    LOG.debug("Remove principals by name in default authorization access controller: empty function");
    LOG.debug("name: " + name);
  }

  public void removePrincipalsByType(String type) throws SqoopException {
    LOG.debug("Remove principals by type in default authorization access controller: empty function");
    LOG.debug("type: " + type);
  }

  public void removePrincipal(MPrincipal principal) throws SqoopException {
    LOG.debug("Remove principal in default authorization access controller: empty function");
    LOG.debug("principal: " + principal.toString());
  }

  public void grantRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    LOG.debug("Grant role in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MRole role : roles) {
      LOG.debug("role: " + role.toString());
    }
  }

  public void revokeRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    LOG.debug("Revoke role in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MRole role : roles) {
      LOG.debug("role: " + role.toString());
    }
  }

  /**
   * Resource related function
   */
  public List<MResource> getAllResources() throws SqoopException {
    LOG.debug("Get all resources in default authorization access controller: return null");
    return null;
  }

  public List<MResource> getResourcesByType(String type) throws SqoopException {
    LOG.debug("Get resources by type in default authorization access controller: return null");
    LOG.debug("type: " + type);
    return null;
  }

  public MResource getResource(String name, String type) throws SqoopException {
    LOG.debug("Get resource in default authorization access controller: return null");
    LOG.debug("name: " + name + ", type: " + type);
    return null;
  }

  public void createResource(String name, String type) throws SqoopException {
    LOG.debug("Create resource in default authorization access controller: empty function");
    LOG.debug("name: " + name + ", type: " + type);
  }

  public void updateResource(MResource old_resource, MResource new_resource) throws SqoopException {
    LOG.debug("Update resource in default authorization access controller: empty function");
    LOG.debug("old_resource: " + old_resource + ", new_resource: " + new_resource);
  }

  public void removeResourcesByType(String type) throws SqoopException {
    LOG.debug("Remove resource by type in default authorization access controller: empty function");
    LOG.debug("type: " + type);
  }

  public void removeResource(MResource resource) throws SqoopException {
    LOG.debug("Remove resource in default authorization access controller: empty function");
    LOG.debug("resource: " + resource.toString());
  }

  /**
   * Privilege related function
   */
  public List<MPrivilege> getAllPrivileges() throws SqoopException {
    LOG.debug("Get all privileges in default authorization access controller: return null");
    return null;
  }

  public MPrivilege getPrivilegeByName(String name) throws SqoopException {
    LOG.debug("Get privileges by name in default authorization access controller: return null");
    LOG.debug("name: " + name);
    return null;
  }

  public List<MPrivilege> getPrivilegesByResource(MResource resource) throws SqoopException {
    LOG.debug("Get privileges by resource in default authorization access controller: return null");
    LOG.debug("resource: " + resource.toString());
    return null;
  }

  public List<MPrivilege> getPrivilegesByRole(MRole role) throws SqoopException {
    LOG.debug("Get privileges by role in default authorization access controller: return null");
    LOG.debug("role: " + role.toString());
    return null;
  }

  public void createPrivilege(String name, MResource resource, String action, boolean with_grant_option) throws SqoopException {
    LOG.debug("Create privilege in default authorization access controller: empty function");
    LOG.debug("name: " + name + ", resource: " + resource.toString() + ", action: " + action + ", with grant option: " + with_grant_option);
  }

  public void updatePrivilege(MPrivilege old_privilege, MPrivilege new_privilege) throws SqoopException {
    LOG.debug("Update privilege in default authorization access controller: empty function");
    LOG.debug("old_privilege: " + old_privilege + ", new_privilege: " + new_privilege);
  }

  public void removePrivilege(String name) throws SqoopException {
    LOG.debug("Remove privilege in default authorization access controller: empty function");
    LOG.debug("name: " + name);
  }

  public void removePrivilegesByResource(MResource resource) throws SqoopException {
    LOG.debug("Remove privileges by resource in default authorization access controller: empty function");
    LOG.debug("resource: " + resource.toString());
  }

  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    LOG.debug("Grant privileges in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MPrivilege privilege : privileges) {
      LOG.debug("privilege: " + privilege.toString());
    }
  }

  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    LOG.debug("Revoke privileges in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MPrivilege privilege : privileges) {
      LOG.debug("privilege: " + privilege.toString());
    }
  }
}
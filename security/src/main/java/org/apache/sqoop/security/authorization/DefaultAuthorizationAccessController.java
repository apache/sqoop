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
package org.apache.sqoop.security.authorization;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.AuthorizationAccessController;

import java.util.List;

public class DefaultAuthorizationAccessController extends AuthorizationAccessController {

  private static final Logger LOG = Logger.getLogger(DefaultAuthorizationAccessController.class);

  /**
   * Role related function
   */
  @Override
  public void createRole(MRole role) throws SqoopException {
    LOG.debug("Create role in default authorization access controller: empty function");
    LOG.debug("role: " + role.toString());
  }

  @Override
  public void dropRole(MRole role) throws SqoopException {
    LOG.debug("Remove role in default authorization access controller: empty function");
    LOG.debug("role: " + role.toString());
  }

  @Override
  public List<MRole> getAllRoles() throws SqoopException {
    LOG.debug("Get all roles in default authorization access controller: return null");
    return null;
  }

  @Override
  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    LOG.debug("Get roles by principal in default authorization access controller: return null");
    LOG.debug("principal: " + principal.toString());
    return null;
  }

  /**
   * Principal related function
   */
  @Override
  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    LOG.debug("Get principals by role in default authorization access controller: return null");
    LOG.debug("role: " + role.toString());
    return null;
  }

  @Override
  public void grantRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    LOG.debug("Grant role in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MRole role : roles) {
      LOG.debug("role: " + role.toString());
    }
  }

  @Override
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
  @Override
  public void updateResource(MResource old_resource, MResource new_resource) throws SqoopException {
    LOG.debug("Update resource in default authorization access controller: empty function");
    LOG.debug("old_resource: " + old_resource + ", new_resource: " + new_resource);
  }

  @Override
  public void removeResource(MResource resource) throws SqoopException {
    LOG.debug("Remove resource in default authorization access controller: empty function");
    LOG.debug("resource: " + resource.toString());
  }

  /**
   * Privilege related function
   */
  @Override
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal, MResource resource) throws SqoopException {
    LOG.debug("Get privileges by role in default authorization access controller: return null");
    LOG.debug("principal: " + principal.toString());
    if (resource != null) { //Get all privileges on principal
      LOG.debug("resource: " + resource.toString());
    }
    return null;
  }

  @Override
  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    LOG.debug("Grant privileges in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    for (MPrivilege privilege : privileges) {
      LOG.debug("privilege: " + privilege.toString());
    }
  }

  @Override
  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    LOG.debug("Revoke privileges in default authorization access controller: empty function");
    for (MPrincipal principal : principals) {
      LOG.debug("principal: " + principal.toString());
    }
    if (privileges != null) { //Revoke all privileges on principal
      for (MPrivilege privilege : privileges) {
        LOG.debug("privilege: " + privilege.toString());
      }
    }
  }
}
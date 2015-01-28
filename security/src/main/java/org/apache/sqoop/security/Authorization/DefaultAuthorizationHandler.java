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
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.*;

import java.util.List;

public class DefaultAuthorizationHandler extends AuthorizationHandler {

  /**
   * Default authorization access controller
   */
  public static final String DEFAULT_AUTHORIZATION_ACCESS_CONTROLLER = "org.apache.sqoop.security.Authorization.DefaultAuthorizationAccessController";

  /**
   * Default authorization validator
   */
  public static final String DEFAULT_AUTHORIZATION_VALIDATOR = "org.apache.sqoop.security.Authorization.DefaultAuthorizationValidator";

  private static final Logger LOG = Logger.getLogger(DefaultAuthorizationHandler.class);

  protected AuthorizationAccessController authorizationAccessController;

  protected AuthorizationValidator authorizationValidator;

  public AuthorizationValidator getAuthorizationValidator() {
    return authorizationValidator;
  }

  public void setAuthorizationValidator(AuthorizationValidator authorizationValidator) {
    this.authorizationValidator = authorizationValidator;
  }

  public AuthorizationAccessController getAuthorizationAccessController() {
    return authorizationAccessController;
  }

  public void setAuthorizationAccessController(AuthorizationAccessController authorizationAccessController) {
    this.authorizationAccessController = authorizationAccessController;
  }

  public void doInitialize() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    MapContext mapContext = SqoopConfiguration.getInstance().getContext();
    String accessController = mapContext.getString(
            SecurityConstants.AUTHORIZATION_ACCESS_CONTROLLER,
            DEFAULT_AUTHORIZATION_ACCESS_CONTROLLER).trim();
    this.authorizationAccessController = SecurityFactory.getAuthorizationAccessController(accessController);

    String validator = mapContext.getString(
            SecurityConstants.AUTHORIZATION_VALIDATOR,
            DEFAULT_AUTHORIZATION_VALIDATOR).trim();
    this.authorizationValidator = SecurityFactory.getAuthorizationValidator(validator);
  }

  /**
   * Role related function
   */
  public List<MRole> getAllRoles() throws SqoopException {
    return this.authorizationAccessController.getAllRoles();
  }

  public MRole getRole(String name) throws SqoopException {
    return this.authorizationAccessController.getRole(name);
  }

  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    return this.authorizationAccessController.getRolesByPrincipal(principal);
  }

  public List<MRole> getRolesByPrivilege(MPrivilege privilege) throws SqoopException {
    return this.authorizationAccessController.getRolesByPrivilege(privilege);
  }

  public void createRole(String name) throws SqoopException {
    this.authorizationAccessController.createRole(name);
  }

  public void updateRole(String old_name, String new_name) throws SqoopException {
    this.authorizationAccessController.updateRole(old_name, new_name);
  }

  public void removeRole(String name) throws SqoopException {
    this.authorizationAccessController.removeRole(name);
  }

  /**
   * Principal related function
   */
  public List<MPrincipal> getAllPrincipals() throws SqoopException {
    return this.authorizationAccessController.getAllPrincipals();
  }

  public List<MPrincipal> getPrincipalsByName(String name) throws SqoopException {
    return this.authorizationAccessController.getPrincipalsByName(name);
  }

  public List<MPrincipal> getPrincipalsByType(String type) throws SqoopException {
    return this.authorizationAccessController.getPrincipalsByType(type);
  }

  public MPrincipal getPrincipal(String name, String type) throws SqoopException {
    return this.authorizationAccessController.getPrincipal(name, type);
  }

  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    return this.authorizationAccessController.getPrincipalsByRole(role);
  }

  public void createPrincipal(String name, String type) throws SqoopException {
    this.authorizationAccessController.createPrincipal(name, type);
  }

  public void updatePrincipal(MPrincipal old_principal, MPrincipal new_principal) throws SqoopException {
    this.authorizationAccessController.updatePrincipal(old_principal, new_principal);
  }

  public void removePrincipalsByName(String name) throws SqoopException {
    this.authorizationAccessController.removePrincipalsByName(name);
  }

  public void removePrincipalsByType(String type) throws SqoopException {
    this.authorizationAccessController.removePrincipalsByType(type);
  }

  public void removePrincipal(MPrincipal principal) throws SqoopException {
    this.authorizationAccessController.removePrincipal(principal);
  }

  public void grantRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    this.authorizationAccessController.grantRole(principals, roles);
  }

  public void revokeRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    this.authorizationAccessController.revokeRole(principals, roles);
  }

  /**
   * Resource related function
   */
  public List<MResource> getAllResources() throws SqoopException {
    return this.authorizationAccessController.getAllResources();
  }

  public List<MResource> getResourcesByType(String type) throws SqoopException {
    return this.authorizationAccessController.getResourcesByType(type);
  }

  public MResource getResource(String name, String type) throws SqoopException {
    return this.authorizationAccessController.getResource(name, type);
  }

  public void createResource(String name, String type) throws SqoopException {
    this.authorizationAccessController.createResource(name, type);
  }

  public void updateResource(MResource old_resource, MResource new_resource) throws SqoopException {
    this.authorizationAccessController.updateResource(old_resource, new_resource);
  }

  public void removeResourcesByType(String type) throws SqoopException {
    this.authorizationAccessController.removeResourcesByType(type);
  }

  public void removeResource(MResource resource) throws SqoopException {
    this.authorizationAccessController.removeResource(resource);
  }

  /**
   * Privilege related function
   */
  public List<MPrivilege> getAllPrivileges() throws SqoopException {
    return this.authorizationAccessController.getAllPrivileges();
  }

  public MPrivilege getPrivilegeByName(String name) throws SqoopException {
    return this.authorizationAccessController.getPrivilegeByName(name);
  }

  public List<MPrivilege> getPrivilegesByResource(MResource resource) throws SqoopException {
    return this.authorizationAccessController.getPrivilegesByResource(resource);
  }

  public List<MPrivilege> getPrivilegesByRole(MRole role) throws SqoopException {
    return this.authorizationAccessController.getPrivilegesByRole(role);
  }

  public void createPrivilege(String name, MResource resource, String action, boolean with_grant_option) throws SqoopException {
    this.authorizationAccessController.createPrivilege(name, resource, action, with_grant_option);
  }

  public void updatePrivilege(MPrivilege old_privilege, MPrivilege new_privilege) throws SqoopException {
    this.authorizationAccessController.updatePrivilege(old_privilege, new_privilege);
  }

  public void removePrivilege(String name) throws SqoopException {
    this.authorizationAccessController.removePrivilege(name);
  }

  public void removePrivilegesByResource(MResource resource) throws SqoopException {
    this.authorizationAccessController.removePrivilegesByResource(resource);
  }

  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationAccessController.grantPrivileges(principals, privileges);
  }

  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationAccessController.revokePrivileges(principals, privileges);
  }

  public void checkPrivileges(MPrincipal principal, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationValidator.checkPrivileges(principal, privileges);
  }
}
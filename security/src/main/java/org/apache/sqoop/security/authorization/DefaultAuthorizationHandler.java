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
  public static final String DEFAULT_AUTHORIZATION_ACCESS_CONTROLLER = "org.apache.sqoop.security.authorization.DefaultAuthorizationAccessController";

  /**
   * Default authorization validator
   */
  public static final String DEFAULT_AUTHORIZATION_VALIDATOR = "org.apache.sqoop.security.authorization.DefaultAuthorizationValidator";

  private static final Logger LOG = Logger.getLogger(DefaultAuthorizationHandler.class);

  protected AuthorizationAccessController authorizationAccessController;

  protected AuthorizationValidator authorizationValidator;

  protected AuthenticationProvider authenticationProvider;

  protected String serverName;

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

  public AuthenticationProvider getAuthenticationProvider() {
    return authenticationProvider;
  }

  public void setAuthenticationProvider(AuthenticationProvider authenticationProvider) {
    this.authenticationProvider = authenticationProvider;
  }

  @Override
  public void doInitialize(AuthenticationProvider provider, String serverName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    MapContext mapContext = SqoopConfiguration.getInstance().getContext();
    String accessController = mapContext.getString(
            SecurityConstants.AUTHORIZATION_ACCESS_CONTROLLER,
            DEFAULT_AUTHORIZATION_ACCESS_CONTROLLER).trim();
    this.authorizationAccessController = SecurityFactory.getAuthorizationAccessController(accessController);

    String validator = mapContext.getString(
            SecurityConstants.AUTHORIZATION_VALIDATOR,
            DEFAULT_AUTHORIZATION_VALIDATOR).trim();
    this.authorizationValidator = SecurityFactory.getAuthorizationValidator(validator);

    this.authenticationProvider = provider;
    this.serverName = serverName;
  }

  /**
   * Role related function
   */
  @Override
  public void createRole(MRole role) throws SqoopException {
    this.authorizationAccessController.createRole(role);
  }

  @Override
  public void dropRole(MRole role) throws SqoopException {
    this.authorizationAccessController.dropRole(role);
  }

  @Override
  public List<MRole> getAllRoles() throws SqoopException {
    return this.authorizationAccessController.getAllRoles();
  }

  @Override
  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    return this.authorizationAccessController.getRolesByPrincipal(principal);
  }

  /**
   * Principal related function
   */
  @Override
  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    return this.authorizationAccessController.getPrincipalsByRole(role);
  }

  @Override
  public void grantRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    this.authorizationAccessController.grantRole(principals, roles);
  }

  @Override
  public void revokeRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException {
    this.authorizationAccessController.revokeRole(principals, roles);
  }

  /**
   * Resource related function
   */
  @Override
  public void updateResource(MResource old_resource, MResource new_resource) throws SqoopException {
    this.authorizationAccessController.updateResource(old_resource, new_resource);
  }

  @Override
  public void removeResource(MResource resource) throws SqoopException {
    this.authorizationAccessController.removeResource(resource);
  }

  /**
   * Privilege related function
   */
  @Override
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal, MResource resource) throws SqoopException {
    return this.authorizationAccessController.getPrivilegesByPrincipal(principal, resource);
  }

  @Override
  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationAccessController.grantPrivileges(principals, privileges);
  }

  @Override
  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationAccessController.revokePrivileges(principals, privileges);
  }

  /**
   * Validator related function
   */
  @Override
  public void checkPrivileges(MPrincipal principal, List<MPrivilege> privileges) throws SqoopException {
    this.authorizationValidator.checkPrivileges(principal, privileges);
  }
}
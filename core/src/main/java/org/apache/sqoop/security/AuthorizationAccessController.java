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
package org.apache.sqoop.security;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;

import java.util.List;

/***
 * AuthorizationAccessController is responsible for managing access rule and principal.
 */
public abstract class AuthorizationAccessController {

  /**
   * Role related function
   */
  public abstract List<MRole> getAllRoles() throws SqoopException;

  public abstract MRole getRole(String name) throws SqoopException;

  public abstract List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException;

  public abstract List<MRole> getRolesByPrivilege(MPrivilege privilege) throws SqoopException;

  public abstract void createRole(String name) throws SqoopException;

  public abstract void updateRole(String old_name, String new_name) throws SqoopException;

  public abstract void removeRole(String name) throws SqoopException;

  /**
   * Principal related function
   */
  public abstract List<MPrincipal> getAllPrincipals() throws SqoopException;

  public abstract List<MPrincipal> getPrincipalsByName(String name) throws SqoopException;

  public abstract List<MPrincipal> getPrincipalsByType(String type) throws SqoopException;

  public abstract MPrincipal getPrincipal(String name, String type) throws SqoopException;

  public abstract List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException;

  public abstract void createPrincipal(String name, String type) throws SqoopException;

  public abstract void updatePrincipal(MPrincipal old_principal, MPrincipal new_principal) throws SqoopException;

  public abstract void removePrincipalsByName(String name) throws SqoopException;

  public abstract void removePrincipalsByType(String type) throws SqoopException;

  public abstract void removePrincipal(MPrincipal principal) throws SqoopException;

  public abstract void grantRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException;

  public abstract void revokeRole(List<MPrincipal> principals, List<MRole> roles) throws SqoopException;

  /**
   * Resource related function
   */
  public abstract List<MResource> getAllResources() throws SqoopException;

  public abstract List<MResource> getResourcesByType(String type) throws SqoopException;

  public abstract MResource getResource(String name, String type) throws SqoopException;

  public abstract void createResource(String name, String type) throws SqoopException;

  public abstract void updateResource(MResource old_resource, MResource new_resource) throws SqoopException;

  public abstract void removeResourcesByType(String type) throws SqoopException;

  public abstract void removeResource(MResource resource) throws SqoopException;

  /**
   * Privilege related function
   */
  public abstract List<MPrivilege> getAllPrivileges() throws SqoopException;

  public abstract MPrivilege getPrivilegeByName(String name) throws SqoopException;

  public abstract List<MPrivilege> getPrivilegesByResource(MResource resource) throws SqoopException;

  public abstract List<MPrivilege> getPrivilegesByRole(MRole role) throws SqoopException;

  public abstract void createPrivilege(String name, MResource resource, String action, boolean with_grant_option) throws SqoopException;

  public abstract void updatePrivilege(MPrivilege old_privilege, MPrivilege new_privilege) throws SqoopException;

  public abstract void removePrivilege(String name) throws SqoopException;

  public abstract void removePrivilegesByResource(MResource resource) throws SqoopException;

  public abstract void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException;

  public abstract void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges) throws SqoopException;
}
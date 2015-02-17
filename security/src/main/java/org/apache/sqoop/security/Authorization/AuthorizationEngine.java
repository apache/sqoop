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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.*;
import org.apache.sqoop.security.AuthorizationHandler;
import org.apache.sqoop.security.AuthorizationManager;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class AuthorizationEngine {

  private static final Logger LOG = Logger.getLogger(AuthorizationEngine.class);

  /**
   * Role type
   */
  public enum RoleType {
    USER, GROUP, ROLE
  }

  /**
   * Resource type
   */
  public enum ResourceType {
    SERVER, CONNECTOR, LINK, JOB
  }

  /**
   * Action type in Privilege
   */
  public enum PrivilegeActionType {
    ALL, READ, WRITE
  }

  /**
   * Filter resources, get all valid resources from all resources
   */
  public static <T extends MPersistableEntity> List<T> filterResource(final ResourceType type, List<T> resources) throws SqoopException {
    Collection<T> collection = Collections2.filter(resources, new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        try {
          String name = String.valueOf(input.getPersistenceId());
          checkPrivilege(getPrivilege(type, name, PrivilegeActionType.READ));
          // add valid resource
          return true;
        } catch (Exception e) {
          //do not add into result if invalid resource
          return false;
        }
      }
    });
    return Lists.newArrayList(collection);
  }

  /**
   * Link related function
   */
  public static void createLink(String connectorId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.CONNECTOR, connectorId, PrivilegeActionType.READ));
  }

  public static void updateLink(String connectorId, String linkId) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(ResourceType.CONNECTOR, connectorId, PrivilegeActionType.READ);
    MPrivilege privilege2 = getPrivilege(ResourceType.LINK, linkId, PrivilegeActionType.WRITE);
    checkPrivilege(privilege1, privilege2);
  }

  public static void deleteLink(String linkId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.LINK, linkId, PrivilegeActionType.WRITE));
  }

  public static void enableDisableLink(String linkId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.LINK, linkId, PrivilegeActionType.WRITE));
  }

  /**
   * Job related function
   */
  public static void createJob(String linkId1, String linkId2) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(ResourceType.LINK, linkId1, PrivilegeActionType.READ);
    MPrivilege privilege2 = getPrivilege(ResourceType.LINK, linkId2, PrivilegeActionType.READ);
    checkPrivilege(privilege1, privilege2);
  }

  public static void updateJob(String linkId1, String linkId2, String jobId) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(ResourceType.LINK, linkId1, PrivilegeActionType.READ);
    MPrivilege privilege2 = getPrivilege(ResourceType.LINK, linkId2, PrivilegeActionType.READ);
    MPrivilege privilege3 = getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.WRITE);
    checkPrivilege(privilege1, privilege2, privilege3);
  }

  public static void deleteJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.WRITE));
  }

  public static void enableDisableJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.WRITE));
  }

  public static void startJob(String jobId) throws SqoopException {
    ;
    checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.WRITE));
  }

  public static void stopJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.WRITE));
  }

  public static void statusJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.READ));
  }

  /**
   * Filter resources, get all valid resources from all resources
   */
  public static List<MSubmission> filterSubmission(List<MSubmission> submissions) throws SqoopException {
    Collection<MSubmission> collection = Collections2.filter(submissions, new Predicate<MSubmission>() {
      @Override
      public boolean apply(MSubmission input) {
        try {
          String jobId = String.valueOf(input.getJobId());
          checkPrivilege(getPrivilege(ResourceType.JOB, jobId, PrivilegeActionType.READ));
          // add valid submission
          return true;
        } catch (Exception e) {
          //do not add into result if invalid submission
          return false;
        }
      }
    });
    return Lists.newArrayList(collection);
  }

  /**
   * Help function
   */
  private static MPrivilege getPrivilege(ResourceType resourceType,
                                         String resourceId,
                                         PrivilegeActionType privilegeActionType) {
    return new MPrivilege(new MResource(resourceId, resourceType.name()), privilegeActionType.name(), false);
  }

  private static void checkPrivilege(MPrivilege... privileges) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    UserGroupInformation user = HttpUserGroupInformation.get();
    String user_name = user == null ? StringUtils.EMPTY : user.getUserName();
    MPrincipal principal = new MPrincipal(user_name, RoleType.USER.name());
    handler.checkPrivileges(principal, Arrays.asList(privileges));
  }
}
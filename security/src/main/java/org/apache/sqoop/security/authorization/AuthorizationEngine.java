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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.*;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.security.AuthorizationHandler;
import org.apache.sqoop.security.AuthorizationManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AuthorizationEngine {

  private static final Logger LOG = Logger.getLogger(AuthorizationEngine.class);

  /**
   * Filter resources, get all valid resources from all resources
   */
  public static <T extends MPersistableEntity> List<T> filterResource(final MResource.TYPE type, List<T> resources) throws SqoopException {
    Collection<T> collection = Collections2.filter(resources, new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        try {
          String name = String.valueOf(input.getPersistenceId());
          checkPrivilege(getPrivilege(type, name, MPrivilege.ACTION.READ));
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
   * Connector related function
   */
  public static void readConnector(String connectorId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.CONNECTOR, connectorId, MPrivilege.ACTION.READ));
  }

  /**
   * Link related function
   */
  public static void readLink(String linkId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.LINK, linkId, MPrivilege.ACTION.READ));
  }

  public static void createLink(String connectorId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.CONNECTOR, connectorId, MPrivilege.ACTION.READ));
  }

  public static void updateLink(String connectorId, String linkId) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.CONNECTOR, connectorId, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkId, MPrivilege.ACTION.WRITE);
    checkPrivilege(privilege1, privilege2);
  }

  public static void deleteLink(String linkId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.LINK, linkId, MPrivilege.ACTION.WRITE));
  }

  public static void enableDisableLink(String linkId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.LINK, linkId, MPrivilege.ACTION.WRITE));
  }

  /**
   * Job related function
   */
  public static void readJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.READ));
  }

  public static void createJob(String linkId1, String linkId2) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.LINK, linkId1, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkId2, MPrivilege.ACTION.READ);
    checkPrivilege(privilege1, privilege2);
  }

  public static void updateJob(String linkId1, String linkId2, String jobId) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.LINK, linkId1, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkId2, MPrivilege.ACTION.READ);
    MPrivilege privilege3 = getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.WRITE);
    checkPrivilege(privilege1, privilege2, privilege3);
  }

  public static void deleteJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.WRITE));
  }

  public static void enableDisableJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.WRITE));
  }

  public static void startJob(String jobId) throws SqoopException {
    ;
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.WRITE));
  }

  public static void stopJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.WRITE));
  }

  public static void statusJob(String jobId) throws SqoopException {
    checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.READ));
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
          checkPrivilege(getPrivilege(MResource.TYPE.JOB, jobId, MPrivilege.ACTION.READ));
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
  private static MPrivilege getPrivilege(MResource.TYPE resourceType,
                                         String resourceId,
                                         MPrivilege.ACTION privilegeAction) {
    return new MPrivilege(new MResource(resourceId, resourceType), privilegeAction, false);
  }

  private static void checkPrivilege(MPrivilege... privileges) {
    AuthorizationHandler handler = AuthorizationManager.getAuthorizationHandler();
    UserGroupInformation user = HttpUserGroupInformation.get();
    String user_name = user == null ? StringUtils.EMPTY : user.getShortUserName();
    MPrincipal principal = new MPrincipal(user_name, MPrincipal.TYPE.USER);

    // SQOOP-2256: Hack code, do not check privilege when the user is the creator
    // If the user is the owner/creator of this resource, then privilege will
    // not be checked. It is a hack code for the time being. The concept of
    // "Owner" will be added in the future and this code will be removed.
    ArrayList<MPrivilege> privilegesNeedCheck = new ArrayList<MPrivilege>();
    for (MPrivilege privilege : privileges) {
      Repository repository = RepositoryManager.getInstance().getRepository();
      if (MResource.TYPE.LINK.name().equalsIgnoreCase(privilege.getResource().getType())) {
        MLink link = repository.findLink(Long.valueOf(privilege.getResource().getName()));
        if (!user_name.equals(link.getCreationUser())) {
          privilegesNeedCheck.add(privilege);
        }
      } else if (MResource.TYPE.JOB.name().equalsIgnoreCase(privilege.getResource().getType())) {
        MJob job = repository.findJob(Long.valueOf(privilege.getResource().getName()));
        if (!user_name.equals(job.getCreationUser())) {
          privilegesNeedCheck.add(privilege);
        }
      } else {
        privilegesNeedCheck.add(privilege);
      }
    }

    handler.checkPrivileges(principal, privilegesNeedCheck);
  }
}
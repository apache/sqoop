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

  private static String getResourceName(MResource.TYPE resourceType, long resourceId) {
    Repository repository = RepositoryManager.getInstance().getRepository();

    switch (resourceType) {
    case CONNECTOR:
      return repository.findConnector(resourceId).getUniqueName();
    case LINK:
      return repository.findLink(resourceId).getName();
    case JOB:
      return repository.findJob(resourceId).getName();
    }

    return null;
  }

  /**
   * Filter resources, get all valid resources from all resources
   */
  public static <T extends MPersistableEntity> List<T> filterResource(final String doUserName, final MResource.TYPE type, List<T> resources) throws SqoopException {
    Collection<T> collection = Collections2.filter(resources, new Predicate<T>() {
      @Override
      public boolean apply(T input) {
        try {
          String name = getResourceName(type, input.getPersistenceId());
          checkPrivilege(doUserName, getPrivilege(type, name, MPrivilege.ACTION.READ));
          // add valid resource
          return true;
        } catch (RuntimeException e) {
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
  public static void readConnector(String doUserName, String connectorName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.CONNECTOR, connectorName, MPrivilege.ACTION.READ));
  }

  /**
   * Link related function
   */
  public static void readLink(String doUserName, String linkName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.LINK, linkName, MPrivilege.ACTION.READ));
  }

  public static void createLink(String doUserName, String connectorName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.CONNECTOR, connectorName, MPrivilege.ACTION.READ));
  }

  public static void updateLink(String doUserName, String connectorName, String linkName) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.CONNECTOR, connectorName, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkName, MPrivilege.ACTION.WRITE);
    checkPrivilege(doUserName, privilege1, privilege2);
  }

  public static void deleteLink(String doUserName, String linkName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.LINK, linkName, MPrivilege.ACTION.WRITE));
  }

  public static void enableDisableLink(String doUserName, String linkName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.LINK, linkName, MPrivilege.ACTION.WRITE));
  }

  /**
   * Job related function
   */
  public static void readJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.READ));
  }

  public static void createJob(String doUserName, String linkName1, String linkName2) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.LINK, linkName1, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkName2, MPrivilege.ACTION.READ);
    checkPrivilege(doUserName, privilege1, privilege2);
  }

  public static void updateJob(String doUserName, String linkName1, String linkName2, String jobName) throws SqoopException {
    MPrivilege privilege1 = getPrivilege(MResource.TYPE.LINK, linkName1, MPrivilege.ACTION.READ);
    MPrivilege privilege2 = getPrivilege(MResource.TYPE.LINK, linkName2, MPrivilege.ACTION.READ);
    MPrivilege privilege3 = getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.WRITE);
    checkPrivilege(doUserName, privilege1, privilege2, privilege3);
  }

  public static void deleteJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.WRITE));
  }

  public static void enableDisableJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.WRITE));
  }

  public static void startJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.WRITE));
  }

  public static void stopJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.WRITE));
  }

  public static void statusJob(String doUserName, String jobName) throws SqoopException {
    checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.READ));
  }

  /**
   * Filter resources, get all valid resources from all resources
   */
  public static List<MSubmission> filterSubmission(final String doUserName, List<MSubmission> submissions) throws SqoopException {
    Collection<MSubmission> collection = Collections2.filter(submissions, new Predicate<MSubmission>() {
      @Override
      public boolean apply(MSubmission input) {
        try {
          String jobName = getResourceName(MResource.TYPE.JOB, input.getJobId());
          checkPrivilege(doUserName, getPrivilege(MResource.TYPE.JOB, jobName, MPrivilege.ACTION.READ));
          // add valid submission
          return true;
        } catch (RuntimeException e) {
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
                                         String resourceName,
                                         MPrivilege.ACTION privilegeAction) {
    return new MPrivilege(new MResource(resourceName, resourceType), privilegeAction, false);
  }

  private static void checkPrivilege(String doUserName, MPrivilege... privileges) {
    AuthorizationHandler handler = AuthorizationManager.getInstance().getAuthorizationHandler();

    MPrincipal principal = new MPrincipal(doUserName, MPrincipal.TYPE.USER);

    // SQOOP-2256: Hack code, do not check privilege when the user is the creator
    // If the user is the owner/creator of this resource, then privilege will
    // not be checked. It is a hack code for the time being. The concept of
    // "Owner" will be added in the future and this code will be removed.
    ArrayList<MPrivilege> privilegesNeedCheck = new ArrayList<MPrivilege>();
    for (MPrivilege privilege : privileges) {
      Repository repository = RepositoryManager.getInstance().getRepository();
      if (MResource.TYPE.LINK.name().equalsIgnoreCase(privilege.getResource().getType())) {
        MLink link = repository.findLink(privilege.getResource().getName());
        if (!doUserName.equals(link.getCreationUser())) {
          privilegesNeedCheck.add(privilege);
        }
      } else if (MResource.TYPE.JOB.name().equalsIgnoreCase(privilege.getResource().getType())) {
        MJob job = repository.findJob(privilege.getResource().getName());
        if (!doUserName.equals(job.getCreationUser())) {
          privilegesNeedCheck.add(privilege);
        }
      } else {
        privilegesNeedCheck.add(privilege);
      }
    }

    handler.checkPrivileges(principal, privilegesNeedCheck);
  }
}
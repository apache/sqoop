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

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.server.common.ServerError;

public class HandlerUtils {

  public static long getJobIdFromIdentifier(String identifier, Repository repository) {
    // support jobName or jobId for the api
    // NOTE: jobId is a fallback for older sqoop clients if any, since we want
    // to primarily use unique jobNames
    long jobId;
    if (repository.findJob(identifier) != null) {
      jobId = repository.findJob(identifier).getPersistenceId();
    } else {
      try {
        jobId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid job: " + identifier
            + " requested");
      }
    }
    return jobId;
  }

  public static long getLinkIdFromIdentifier(String identifier, Repository repository) {
    // support linkName or linkId for the api
    // NOTE: linkId is a fallback for older sqoop clients if any, since we want
    // to primarily use unique linkNames
    long linkId;
    if (repository.findLink(identifier) != null) {
      linkId = repository.findLink(identifier).getPersistenceId();
    } else {
      try {
        linkId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid link: " + identifier
            + " requested");
      }
    }
    return linkId;
  }

  public static long getConnectorIdFromIdentifier(String identifier) {
    long connectorId;
    if (ConnectorManager.getInstance().getConnectorId(identifier) != null) {
      connectorId = ConnectorManager.getInstance().getConnectorId(identifier);
    } else {
      try {
        connectorId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid connector: " + identifier
            + " requested");
      }
    }
    return connectorId;
  }

}

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
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.common.ServerError;

public class HandlerUtils {

  public static MJob getJobFromIdentifier(String identifier) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MJob job = repository.findJob(identifier);
    if (job == null) {
      throw new SqoopException(ServerError.SERVER_0006, "Job: " + identifier
              + " doesn't exist");
    }
    return job;
  }

  public static MLink getLinkFromLinkName(String linkName) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MLink link = repository.findLink(linkName);
    if (link == null) {
      throw new SqoopException(ServerError.SERVER_0006, "Invalid link name: " + linkName
              + " doesn't exist");
    }
    return link;
  }

  public static MLink getLinkFromLinkId(Long linkId) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MLink link = repository.findLink(linkId);
    if (link == null) {
      throw new SqoopException(ServerError.SERVER_0006, "Invalid link id: " + linkId
              + " doesn't exist");
    }
    return link;
  }

  public static MConnector getConnectorFromConnectorName(String connectorName) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MConnector connector = repository.findConnector(connectorName);
    if (connector == null) {
      // TODO: get the connector by id from public API should be dropped.
      try {
        connector = repository.findConnector(Long.parseLong(connectorName));
      } catch (NumberFormatException nfe) {
        // do nothing, connector should be null, and will thrown exception in the following.
      }
    }
    if (connector == null) {
      throw new SqoopException(ServerError.SERVER_0006, "Connector: " + connectorName
              + " doesn't exist");
    }
    return connector;
  }

  public static MConnector getConnectorFromConnectorId(Long connectorId) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MConnector connector = repository.findConnector(connectorId);
    if (connector == null) {
      throw new SqoopException(ServerError.SERVER_0006, "Connector id: " + connectorId
              + " doesn't exist");
    }
    return connector;
  }
}

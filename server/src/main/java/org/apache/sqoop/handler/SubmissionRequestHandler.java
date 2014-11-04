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

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.SubmissionsBean;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestContext.Method;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;

public class SubmissionRequestHandler implements RequestHandler {

  private static final Logger LOG = Logger.getLogger(SubmissionRequestHandler.class);

  public SubmissionRequestHandler() {
    LOG.info("SubmissionRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {

    // submission only support GET requests
    if (ctx.getMethod() != Method.GET) {
      throw new SqoopException(ServerError.SERVER_0002, "Unsupported HTTP method for connector:"
          + ctx.getMethod());
    }
    String identifier = ctx.getLastURLElement();
    Repository repository = RepositoryManager.getInstance().getRepository();
    // links by connector ordered by updated time
    // hence the latest submission is on the top
    if (ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM) != null) {
      identifier = ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM);
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "submissionsByJob", identifier);
      if (repository.findJob(identifier) != null) {
        long jobId = repository.findJob(identifier).getPersistenceId();
        return getSubmissionsForJob(jobId);
      } else {
        // this means name nor Id existed
        throw new SqoopException(ServerError.SERVER_0005, "Invalid job: " + identifier
            + " name given");
      }
    } else {
      // all submissions in the system
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "submissions", "all");
      return getSubmissions();
    }
  }

  private JsonBean getSubmissions() {
    List<MSubmission> submissions = RepositoryManager.getInstance().getRepository()
        .findSubmissions();
    return new SubmissionsBean(submissions);
  }

  private JsonBean getSubmissionsForJob(long jid) {
    List<MSubmission> submissions = RepositoryManager.getInstance().getRepository()
        .findSubmissionsForJob(jid);
    return new SubmissionsBean(submissions);
  }
}

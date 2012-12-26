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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;

/**
 * Submission request handler is supporting following resources:
 *
 * GET /v1/submission/action/:jid
 * Get status of last submission for job with id :jid
 *
 * POST /v1/submission/action/:jid
 * Create new submission for job with id :jid
 *
 * DELETE /v1/submission/action/:jid
 * Stop last submission for job with id :jid
 *
 * GET /v1/submission/notification/:jid
 * Notification endpoint to get job status outside normal interval
 *
 * Possible additions in the future: /v1/submission/history/* for history.
 */
public class SubmissionRequestHandler implements RequestHandler {

  private final Logger logger = Logger.getLogger(getClass());

  public SubmissionRequestHandler() {
    logger.info("SubmissionRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    String[] urlElements = ctx.getUrlElements();
    if (urlElements.length < 2) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Invalid URL, too few arguments for this servlet.");
    }

    // Let's check
    int length = urlElements.length;
    String action = urlElements[length - 2];

    if(action.equals("action")) {
      return handleActionEvent(ctx, urlElements[length - 1]);
    }

    if(action.equals("notification")) {
      return handleNotification(ctx, urlElements[length - 1]);
    }

    throw new SqoopException(ServerError.SERVER_0003,
      "Do not know what to do.");
  }

  private JsonBean handleNotification(RequestContext ctx, String sjid) {
    logger.debug("Received notification request for job " + sjid);
    FrameworkManager.getInstance().status(Long.parseLong(sjid));
    return JsonBean.EMPTY_BEAN;
  }

  private JsonBean handleActionEvent(RequestContext ctx, String sjid) {
    long jid = Long.parseLong(sjid);

    switch (ctx.getMethod()) {
      case GET:
        return submissionStatus(jid);
      case POST:
        // TODO: This should be outsourced somewhere more suitable than here
        if(FrameworkManager.getInstance().getNotificationBaseUrl() == null) {
          String url = ctx.getRequest().getRequestURL().toString();
          FrameworkManager.getInstance().setNotificationBaseUrl(
            url.split("v1")[0] + "/v1/submission/notification/");
        }
        return submissionSubmit(jid);
      case DELETE:
        return submissionStop(jid);
    }

    return null;
  }

  private JsonBean submissionStop(long jid) {
    MSubmission submission = FrameworkManager.getInstance().stop(jid);
    return new SubmissionBean(submission);
  }

  private JsonBean submissionSubmit(long jid) {
    MSubmission submission = FrameworkManager.getInstance().submit(jid);
    return new SubmissionBean(submission);
  }

  private JsonBean submissionStatus(long jid) {
    MSubmission submission = FrameworkManager.getInstance().status(jid);
    return new SubmissionBean(submission);
  }
}

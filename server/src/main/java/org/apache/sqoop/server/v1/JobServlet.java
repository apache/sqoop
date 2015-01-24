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
package org.apache.sqoop.server.v1;

import org.apache.sqoop.handler.JobRequestHandler;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.SqoopProtocolServlet;


/**
 * Provides operations for job resource
 *
 * GET /v1/job/{jid}
 * Return details about one particular job with id:jid or about all of
 * them if jid equals to "all"
 *
 * POST /v1/job
 * Create new job
 * POST /v1/job/ with {from-link-id}, {to-link-id} and other job details in the post data
 *  Create job with from and to link
 * PUT /v1/link/ with {from-link-id}, {to-link-id} and other job details in the post data
 *  Edit/Update job for the from and to link
 *
 * PUT /v1/job/{jid} and the job details in the post data
 * Update job with id jid.
 *
 * PUT /v1/job/{jid}/enable
 *  Enable job with id jid
 * PUT /v1/job/{jname}/disable
 *  Enable job with name jname
 *
 * PUT /v1/job/{jid}/disable
 *  Disable job with id jid
 * PUT /v1/job/{jname}/disable
 *  Disable job with name jname
 *
 * DELETE /v1/job/{jid}
 *  Remove job with id jid
 * DELETE /v1/job/{jname}
 *  Remove job with name jname
 *
 * PUT /v1/job/{jid}/submit
 *  Submit job with id jid to create a submission record
 * PUT /v1/job/{jname}/submit
 *  Submit job with name jname to create a submission record
 *
 * PUT /v1/job/{jid}/stop
 *  Abort/Stop last running associated submission with job id jid
 * PUT /v1/job/{jname}/stop
 *  Abort/Stop last running associated submission with job name jname
 *
 * GET /v1/job/{jid}/status
 *  get status of running job with job id jid
 * GET /v1/job/{jname}/status
 *  get status of running job with job name jname
 *
 */
@SuppressWarnings("serial")
public class JobServlet extends SqoopProtocolServlet {

  private RequestHandler jobRequestHandler;

  public JobServlet() {
    jobRequestHandler = new JobRequestHandler();
  }

  @Override
  protected JsonBean handleGetRequest(RequestContext ctx) throws Exception {
    return jobRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePostRequest(RequestContext ctx) throws Exception {
    return jobRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handlePutRequest(RequestContext ctx) throws Exception {
    return jobRequestHandler.handleEvent(ctx);
  }

  @Override
  protected JsonBean handleDeleteRequest(RequestContext ctx) throws Exception {
    return jobRequestHandler.handleEvent(ctx);
  }
}

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
 * GET /v1/job/{jname}
 * Return details about one particular job with name:jname or about all of
 * them if jname equals to "all"
 *
 * POST /v1/job
 * Create new job
 * POST /v1/job/ with {from-link-name}, {to-link-name} and other job details in the post data
 *  Create job with from and to link
 * PUT /v1/link/ with {from-link-name}, {to-link-name} and other job details in the post data
 *  Edit/Update job for the from and to link
 *
 * PUT /v1/job/{jname} and the job details in the post data
 * Update job with name jname.
 *
 * PUT /v1/job/{jname}/enable
 *  Enable job with name jname
 * PUT /v1/job/{jname}/disable
 *  Enable job with name jname
 *
 * DELETE /v1/job/{jname}
 *  Remove job with name jname
 *
 * PUT /v1/job/{jname}/submit
 *  Submit job with name jname to create a submission record
 *
 * PUT /v1/job/{jname}/stop
 *  Abort/Stop last running associated submission with job name jname
 *
 * GET /v1/job/{jname}/status
 *  get status of running job with job name jname
 *
 */
@SuppressWarnings("serial")
public class JobServlet extends SqoopProtocolServlet {
  private static final long serialVersionUID = 1L;

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

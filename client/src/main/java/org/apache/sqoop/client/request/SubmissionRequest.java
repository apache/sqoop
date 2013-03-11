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
package org.apache.sqoop.client.request;

import org.apache.sqoop.json.SubmissionBean;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Provide CRD semantics over RESTfull HTTP API for submissions. Please note
 * that "update" is not supported as client can't update submission status.
 */
public class SubmissionRequest extends  Request {

  public static final String RESOURCE = "v1/submission/";

  public static final String ACTION = RESOURCE + "action/";

  public SubmissionBean read(String serverUrl, Long jid) {
    String response = super.get(serverUrl + ACTION + jid);

    JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

    SubmissionBean submissionBean = new SubmissionBean();
    submissionBean.restore(jsonObject);

    return submissionBean;
  }

  public SubmissionBean create(String serverUrl, Long jid) {
    String response = super.post(serverUrl + ACTION + jid, null);

    SubmissionBean submissionBean = new SubmissionBean();
    submissionBean.restore((JSONObject) JSONValue.parse(response));

    return submissionBean;
  }

  public SubmissionBean delete(String serverUrl, Long id) {
     String response = super.delete(serverUrl + ACTION + id);

    SubmissionBean submissionBean = new SubmissionBean();
    submissionBean.restore((JSONObject) JSONValue.parse(response));

    return submissionBean;
  }
}

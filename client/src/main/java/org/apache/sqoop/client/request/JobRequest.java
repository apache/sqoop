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

import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.MJob;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Provide CRUD semantics over RESTfull HTTP API for jobs. All operations
 * are normally supported.
 */
public class JobRequest extends Request {

  public static final String RESOURCE = "v1/job/";

  private static final String ENABLE = "/enable";
  private static final String DISABLE = "/disable";

  public JobBean read(String serverUrl, Long xid) {
    String response;
    if (xid == null) {
      response = super.get(serverUrl + RESOURCE + "all");
    } else {
      response = super.get(serverUrl + RESOURCE + xid);
    }
    JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

    JobBean jobBean = new JobBean();
    jobBean.restore(jsonObject);

    return jobBean;
  }

  public ValidationResultBean create(String serverUrl, MJob job) {
    JobBean jobBean = new JobBean(job);

    // Extract all form inputs including sensitive inputs
    JSONObject jobJson = jobBean.extract(false);

    String response = super.post(serverUrl + RESOURCE, jobJson.toJSONString());

    ValidationResultBean validationBean = new ValidationResultBean();
    validationBean.restore((JSONObject) JSONValue.parse(response));

    return validationBean;
  }

  public ValidationResultBean update(String serverUrl, MJob job) {
    JobBean jobBean = new JobBean(job);

    // Extract all form inputs including sensitive inputs
    JSONObject jobJson = jobBean.extract(false);

    String response = super.put(serverUrl + RESOURCE + job.getPersistenceId(), jobJson.toJSONString());

    ValidationResultBean validationBean = new ValidationResultBean();
    validationBean.restore((JSONObject) JSONValue.parse(response));

    return validationBean;
  }

  public void delete(String serverUrl, Long id) {
     super.delete(serverUrl + RESOURCE + id);
  }

  public void enable(String serverUrl, Long id, Boolean enabled) {
    if (enabled) {
      super.put(serverUrl + RESOURCE + id + ENABLE, null);
    } else {
      super.put(serverUrl + RESOURCE + id + DISABLE, null);
    }
  }
}

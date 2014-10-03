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

import org.apache.sqoop.json.LinkBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.MLink;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Provide CRUD semantics over RESTfull HTTP API for links. All operations
 * are normally supported.
 */
public class LinkResourceRequest extends ResourceRequest {

  public static final String RESOURCE = "v1/link/";

  private static final String ENABLE = "/enable";
  private static final String DISABLE = "/disable";

  public LinkBean read(String serverUrl, Long xid) {
    String response;
    if (xid == null) {
      response = super.get(serverUrl + RESOURCE + "all");
    } else {
      response = super.get(serverUrl + RESOURCE + xid);
    }
    JSONObject jsonObject = (JSONObject)JSONValue.parse(response);
    LinkBean linkBean = new LinkBean();
    linkBean.restore(jsonObject);
    return linkBean;
  }

  public ValidationResultBean create(String serverUrl, MLink link) {
    LinkBean linkBean = new LinkBean(link);

    // Extract all config inputs including sensitive inputs
    JSONObject linkJson = linkBean.extract(false);
    String response = super.post(serverUrl + RESOURCE, linkJson.toJSONString());
    ValidationResultBean validationBean = new ValidationResultBean();
    validationBean.restore((JSONObject) JSONValue.parse(response));
    return validationBean;
  }

  public ValidationResultBean update(String serverUrl, MLink link) {
    LinkBean linkBean = new LinkBean(link);

    // Extract all config inputs including sensitive inputs
    JSONObject linkJson = linkBean.extract(false);
    String response = super.put(serverUrl + RESOURCE + link.getPersistenceId(), linkJson.toJSONString());
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

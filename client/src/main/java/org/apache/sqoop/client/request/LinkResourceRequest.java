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

import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.LinkBean;
import org.apache.sqoop.json.LinksBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.MLink;
import org.json.simple.JSONObject;

/**
 * Provide CRUD semantics over RESTfull HTTP API for links. All operations
 * are normally supported.
 */
public class LinkResourceRequest extends ResourceRequest {

  public static final String LINK_RESOURCE = "v1/link/";

  private static final String ENABLE = "/enable";
  private static final String DISABLE = "/disable";

  public LinkResourceRequest(){
    super();
  }

  public LinkResourceRequest(DelegationTokenAuthenticatedURL.Token token){
    super(token);
  }

  public LinkBean read(String serverUrl, Long linkId) {
    String response;
    if (linkId == null) {
      response = super.get(serverUrl + LINK_RESOURCE + "all");
    } else {
      response = super.get(serverUrl + LINK_RESOURCE + linkId);
    }
    JSONObject jsonObject = JSONUtils.parse(response);
    // defaults to all
    LinkBean bean = new LinksBean();
    if (linkId != null) {
      bean = new LinkBean();
    }
    bean.restore(jsonObject);
    return bean;
  }

  public ValidationResultBean create(String serverUrl, MLink link) {
    LinkBean linkBean = new LinkBean(link);
    // Extract all config inputs including sensitive inputs
    JSONObject linkJson = linkBean.extract(false);
    String response = super.post(serverUrl + LINK_RESOURCE, linkJson.toJSONString());
    ValidationResultBean validationBean = new ValidationResultBean();
    validationBean.restore(JSONUtils.parse(response));
    return validationBean;
  }

  public ValidationResultBean update(String serverUrl, MLink link) {
    LinkBean linkBean = new LinkBean(link);
    // Extract all config inputs including sensitive inputs
    JSONObject linkJson = linkBean.extract(false);
    String response = super.put(serverUrl + LINK_RESOURCE + link.getPersistenceId(), linkJson.toJSONString());
    ValidationResultBean validationBean = new ValidationResultBean();
    validationBean.restore(JSONUtils.parse(response));
    return validationBean;
  }

  public void delete(String serverUrl, Long id) {
     super.delete(serverUrl + LINK_RESOURCE + id);
  }

  public void enable(String serverUrl, Long id, Boolean enabled) {
    if (enabled) {
      super.put(serverUrl + LINK_RESOURCE + id + ENABLE, null);
    } else {
      super.put(serverUrl + LINK_RESOURCE + id + DISABLE, null);
    }
  }
}

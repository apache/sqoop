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
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.ConnectorsBean;
import org.apache.sqoop.json.JSONUtils;
import org.json.simple.JSONObject;

/**
 * Provide Read semantics over RESTfull HTTP API for connectors. Only read is
 * supported as creation, update and delete might be done only directly on
 * server side.
 */
public class ConnectorResourceRequest extends ResourceRequest {
  public static final String RESOURCE = "v1/connector/";

  public ConnectorResourceRequest(){
    super();
  }

  public ConnectorResourceRequest(DelegationTokenAuthenticatedURL.Token token){
    super(token);
  }

  public ConnectorBean read(String serverUrl, Long cid) {
    String response;
    if (cid == null) {
      response = super.get(serverUrl + RESOURCE + "all");
    } else {
      response = super.get(serverUrl + RESOURCE + cid);
    }
    JSONObject jsonObject = JSONUtils.parse(response);
    // defaults to all
    ConnectorBean bean = new ConnectorsBean();
    if (cid != null) {
      bean = new ConnectorBean();
    }
    bean.restore(jsonObject);
    return bean;
  }
}

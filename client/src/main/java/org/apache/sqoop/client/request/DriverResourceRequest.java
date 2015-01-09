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
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.json.JSONUtils;
import org.json.simple.JSONObject;

/**
 * Provide read semantics over RESTfull HTTP API for driver
 */
public class DriverResourceRequest extends ResourceRequest {

  public static final String RESOURCE = "v1/driver/";

  public DriverResourceRequest(){
    super();
  }

  public DriverResourceRequest(DelegationTokenAuthenticatedURL.Token token){
    super(token);
  }

  public DriverBean read(String serverUrl) {
    String response = null;
    response = super.get(serverUrl + RESOURCE);
    JSONObject jsonObject = JSONUtils.parse(response);
    DriverBean driverBean = new DriverBean();
    driverBean.restore(jsonObject);
    return driverBean;
  }
}

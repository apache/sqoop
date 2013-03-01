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

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SqoopProtocolConstants;
import org.apache.sqoop.json.ThrowableBean;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Locale;

public class Request
{
  private static ServerExceptionFilter serverExceptionFilter;

  static {
    serverExceptionFilter = new ServerExceptionFilter();
  }

  protected Builder getBuilder(String url) {
    Client client = Client.create();
    WebResource resource = client.resource(url);

    // Provide filter that will rebuild exception that is sent from server
    resource.addFilter(serverExceptionFilter);

    return resource
      // Provide name of user executing request.
      .header(SqoopProtocolConstants.HEADER_SQOOP_USERNAME, System.getProperty("user.name"))
      // Sqoop is using JSON for data transfers
      .accept(MediaType.APPLICATION_JSON_TYPE)
      // Transfer client locale to return client specific data
      .acceptLanguage(Locale.getDefault());
  }

  public String get(String url) {
    return getBuilder(url).get(String.class);
  }

  public String post(String url, String data) {
    return getBuilder(url).post(String.class, data);
  }

  public String put(String url, String data) {
    return getBuilder(url).put(String.class, data);
  }

  public String delete(String url) {
    return getBuilder(url).delete(String.class);
  }

  /**
   * Client filter to intercepting exceptions sent by sqoop server and
   * recreating them on client side. Current implementation will create new
   * instance of SqoopException and will attach original error code and message.
   */
  private static class ServerExceptionFilter extends ClientFilter {
    @Override
    public ClientResponse handle(ClientRequest cr) {
      ClientResponse resp = getNext().handle(cr);

      // Special handling for 500 internal server error in case that server
      // has sent us it's exception correctly. We're using default route
      // for all other 500 occurrences.
      if(resp.getClientResponseStatus()
        == ClientResponse.Status.INTERNAL_SERVER_ERROR) {

        if(resp.getHeaders().containsKey(
          SqoopProtocolConstants.HEADER_SQOOP_INTERNAL_ERROR_CODE)) {

          ThrowableBean ex = new ThrowableBean();

          String responseText = resp.getEntity(String.class);
          JSONObject json = (JSONObject) JSONValue.parse(responseText);
          ex.restore(json);

          throw new SqoopException(ClientError.CLIENT_0006, ex.getThrowable());
        }
      }

      return resp;
    }
  }
}

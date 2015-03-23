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
package org.apache.sqoop.server;

import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.security.AuthenticationManager;
import org.apache.sqoop.server.common.ServerError;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Locale;

public class RequestContext {

  /**
   * Enumeration with supported HTTP methods.
   */
  public enum Method {
    GET,
    POST,
    PUT,
    DELETE,
  }

  private final HttpServletRequest request;
  private final HttpServletResponse response;

  public RequestContext(HttpServletRequest req, HttpServletResponse resp) {
    request = req;
    response = resp;
  }

  public HttpServletRequest getRequest() {
    return request;
  }

  public HttpServletResponse getResponse() {
    return response;
  }

  public String getPath() {
    return request.getRequestURL().toString();
  }

  /**
   * Get method that was used for this HTTP request.
   *
   * @return Method that was used
   */
  public Method getMethod() {
    try {
      return Method.valueOf(request.getMethod());
    } catch(IllegalArgumentException ex) {
      throw new SqoopException(ServerError.SERVER_0002,
              "Unsupported HTTP method:" + request.getMethod(), ex);
    }
  }

  /**
   * Return last element of URL.
   *
   * Return text occurring after last "/" character in URL, typically there will
   * be an ID.
   *
   * @return String after last "/" in URL
   */
  public String getLastURLElement() {
    String uri = getRequest().getRequestURI();
    int slash = uri.lastIndexOf('/');
    return uri.substring(slash + 1);
  }

  /**
   * Return all elements in the url as an array
   */
  public String[] getUrlElements() {
    return getRequest().getRequestURI().split("/");
  }

  /**
   * Return a value for given query parameter name
   */
  public String getParameterValue(String name) {
    String[] values = getRequest().getParameterValues(name);
    return values != null ? values[0] : null;
  }

  /**
   * Get locale specified in accept-language HTTP header.
   *
   * @return First specified locale
   */
  public Locale getAcceptLanguageHeader() {
    String lang = request.getHeader("Accept-Language");
    if (lang == null) {
      lang = Locale.getDefault().getLanguage();
    }
    return new Locale(lang);
  }

  /**
   * Get username specified by custom username HTTP header.
   *
   * @return Name of user sending the request
   */
  public String getUserName() {
    if (AuthenticationManager.getAuthenticationHandler().isSecurityEnabled()) {
      return HttpUserGroupInformation.get().getShortUserName();
    } else {
      return request.getParameter(PseudoAuthenticator.USER_NAME);
    }
  }
}

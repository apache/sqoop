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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SqoopProtocolConstants;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.ThrowableBean;
import org.json.simple.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Locale;

/**
 * Represents the sqoop REST resource requests
 */
public class ResourceRequest {
  private static final Logger LOG = Logger.getLogger(ResourceRequest.class);
  private DelegationTokenAuthenticatedURL.Token authToken;

  public ResourceRequest() {
    this.authToken = new DelegationTokenAuthenticatedURL.Token();
  }

  public ResourceRequest(DelegationTokenAuthenticatedURL.Token token) {
    this.authToken = token == null ? new DelegationTokenAuthenticatedURL.Token() : token;
  }
  protected String doHttpRequest(String strURL, String method) {
    return doHttpRequest(strURL, method, "");
  }

  protected String doHttpRequest(String strURL, String method, String data) {
    DataOutputStream wr = null;
    BufferedReader reader = null;
    try {
//    This user name is only in simple mode. In Kerberos mode, this user name will be ignored by Sqoop server and user name in UGI which is authenticated by Kerberos server will be used instead.
      strURL = addUsername(strURL);
      URL url = new URL(strURL);
      HttpURLConnection conn = new DelegationTokenAuthenticatedURL().openConnection(url, authToken);

      conn.setRequestMethod(method);
//      Sqoop is using JSON for data transfers
      conn.setRequestProperty("Accept", MediaType.APPLICATION_JSON);
//      Transfer client locale to return client specific data
      conn.setRequestProperty("Accept-Language", Locale.getDefault().toString());
      if (method.equalsIgnoreCase(HttpMethod.PUT) || method.equalsIgnoreCase(HttpMethod.POST)) {
        conn.setDoOutput(true);
        data = data == null ? "" : data;
        conn.setRequestProperty("Content-Length", Integer.toString(data.getBytes().length));
//        Send request
        wr = new DataOutputStream(conn.getOutputStream());
        wr.writeBytes(data);
        wr.flush();
        wr.close();
      }

      LOG.debug("Status code: " + conn.getResponseCode() + " " + conn.getResponseMessage());
      StringBuilder result = new StringBuilder();
      int responseCode = conn.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_OK) {
        reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line = reader.readLine();
        while (line != null) {
          result.append(line);
          line = reader.readLine();
        }
        reader.close();
      } else if (responseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
        /**
         * Client filter to intercepting exceptions sent by sqoop server and
         * recreating them on client side. Current implementation will create new
         * instance of SqoopException and will attach original error code and message.
         *
         * Special handling for 500 internal server error in case that server
         * has sent us it's exception correctly. We're using default route
         * for all other 500 occurrences.
         */
        if (conn.getHeaderFields().keySet().contains(
                SqoopProtocolConstants.HEADER_SQOOP_INTERNAL_ERROR_CODE)) {

          ThrowableBean ex = new ThrowableBean();

          result = new StringBuilder();
          reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
          String line = reader.readLine();
          while (line != null) {
            result.append(line);
            line = reader.readLine();
          }
          reader.close();

          JSONObject json = JSONUtils.parse(result.toString());
          ex.restore(json);

          throw new SqoopException(ClientError.CLIENT_0001, ex.getThrowable());
        } else {
          result = new StringBuilder();
          reader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
          String line;
          while ((line = reader.readLine()) != null) {
            result.append(line);
            result.append('\n');
          }
          reader.close();

          throw new SqoopException(ClientError.CLIENT_0001, result.toString());
        }
      } else {
        throw new SqoopException(ClientError.CLIENT_0000);
      }
      return result.toString();
    } catch (IOException ex) {
      LOG.trace("ERROR: ", ex);
      throw new SqoopException(ClientError.CLIENT_0000, ex);
    } catch (AuthenticationException ex) {
      LOG.trace("ERROR: ", ex);
      throw new SqoopException(ClientError.CLIENT_0004, ex);
    } finally {
      try {
        if (wr != null) {
          wr.close();
        }
      } catch (IOException e) {
        LOG.trace("Cannot close DataOutputStream.", e);
      }
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOG.trace("Cannot close BufferReader.", e);
      }
    }
  }

  public String get(String url) {
    return doHttpRequest(url, HttpMethod.GET);
  }

  public String post(String url, String data) {
    return doHttpRequest(url, HttpMethod.POST, data);
  }

  public String put(String url, String data) {
    return doHttpRequest(url, HttpMethod.PUT, data);
  }

  public String delete(String url) {
    return doHttpRequest(url, HttpMethod.DELETE);
  }

  public Token<?>[] addDelegationTokens(String strURL, String renewer,
                                        Credentials credentials) throws IOException {
    Token<?>[] tokens = null;
    Text dtService = getDelegationTokenService(strURL);
    Token<?> token = credentials.getToken(dtService);
    if (token == null) {
      URL url = new URL(strURL);
      DelegationTokenAuthenticatedURL authUrl =
              new DelegationTokenAuthenticatedURL(new ConnectionConfigurator() {
                @Override
                public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
                  return conn;
                }
              });
      try {
        token = authUrl.getDelegationToken(url, authToken, renewer);
        if (token != null) {
          credentials.addToken(token.getService(), token);
          tokens = new Token<?>[]{token};
        } else {
          throw new IOException("Got NULL as delegation token");
        }
      } catch (AuthenticationException ex) {
        throw new IOException(ex);
      }
    }
    return tokens;
  }

  private Text getDelegationTokenService(String strURL) throws IOException {
    URL url = new URL(strURL);
    InetSocketAddress addr = new InetSocketAddress(url.getHost(),
            url.getPort());
    Text dtService = SecurityUtil.buildTokenService(addr);
    return dtService;
  }

  public DelegationTokenAuthenticatedURL.Token getAuthToken() {
    return authToken;
  }

  private String addUsername(String strUrl) {
    String paramSeparator = (strUrl.contains("?")) ? "&" : "?";
    strUrl += paramSeparator + PseudoAuthenticator.USER_NAME + "=" + System.getProperty("user.name");
    return strUrl;
  }
}
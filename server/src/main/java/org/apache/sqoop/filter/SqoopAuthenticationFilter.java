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
package org.apache.sqoop.filter;

import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.security.AuthenticationConstants;
import org.apache.sqoop.security.AuthenticationError;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.Properties;

public class SqoopAuthenticationFilter extends AuthenticationFilter {

  @Override
  protected Properties getConfiguration(String configPrefix,
                                        FilterConfig filterConfig) throws ServletException {
    Properties properties = super.getConfiguration(configPrefix, filterConfig);
    MapContext mapContext = SqoopConfiguration.getInstance().getContext();
    String type = mapContext.getString(
        AuthenticationConstants.AUTHENTICATION_TYPE,
        AuthenticationConstants.TYPE.SIMPLE.name()).trim();

    if (type.equalsIgnoreCase(AuthenticationConstants.TYPE.KERBEROS.name())) {
      properties.setProperty(AUTH_TYPE, AuthenticationConstants.TYPE.KERBEROS.name().toLowerCase());

      String keytab = mapContext.getString(
              AuthenticationConstants.AUTHENTICATION_KERBEROS_HTTP_KEYTAB).trim();
      if (keytab.length() == 0) {
        throw new SqoopException(AuthenticationError.AUTH_0005,
                AuthenticationConstants.AUTHENTICATION_KERBEROS_HTTP_KEYTAB);
      }

      String principal = mapContext.getString(
              AuthenticationConstants.AUTHENTICATION_KERBEROS_HTTP_PRINCIPAL).trim();
      if (principal.length() == 0) {
        throw new SqoopException(AuthenticationError.AUTH_0006,
                AuthenticationConstants.AUTHENTICATION_KERBEROS_HTTP_PRINCIPAL);
      }

      String hostPrincipal = "";
      try {
        hostPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
      } catch (IOException e) {
        throw new SqoopException(AuthenticationError.AUTH_0006,
                AuthenticationConstants.AUTHENTICATION_KERBEROS_HTTP_PRINCIPAL);
      }

      properties.setProperty(KerberosAuthenticationHandler.PRINCIPAL, hostPrincipal);
      properties.setProperty(KerberosAuthenticationHandler.KEYTAB, keytab);
    } else if (type.equalsIgnoreCase(AuthenticationConstants.TYPE.SIMPLE.name())) {
      properties.setProperty(AUTH_TYPE, PseudoAuthenticationHandler.class.getName());
      properties.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
          mapContext.getString(AuthenticationConstants.AUTHENTICATION_ANONYMOUS, "true").trim());
    } else {
      throw new SqoopException(AuthenticationError.AUTH_0004, type);
    }

    return properties;
  }
}

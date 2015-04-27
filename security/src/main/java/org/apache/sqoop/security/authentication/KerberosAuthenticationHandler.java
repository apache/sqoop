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
package org.apache.sqoop.security.authentication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.security.AuthenticationHandler;
import org.apache.sqoop.security.SecurityConstants;
import org.apache.sqoop.security.SecurityError;

import java.io.IOException;

public class KerberosAuthenticationHandler extends AuthenticationHandler {

  private static final Logger LOG = Logger.getLogger(KerberosAuthenticationHandler.class);

  /**
   * Principal for Kerberos option value
   */
  private String keytabPrincipal;

  public String getKeytabPrincipal() {
    return keytabPrincipal;
  }

  /**
   * Keytab for Kerberos option value
   */
  private String keytabFile;

  public String getKeytabFile() {
    return keytabFile;
  }

  public void doInitialize() {
    securityEnabled = true;
  }

  public void secureLogin() {
    MapContext mapContext = SqoopConfiguration.getInstance().getContext();
    String keytab = mapContext.getString(
            SecurityConstants.AUTHENTICATION_KERBEROS_KEYTAB).trim();
    if (keytab.length() == 0) {
      throw new SqoopException(SecurityError.AUTH_0001,
              SecurityConstants.AUTHENTICATION_KERBEROS_KEYTAB);
    }
    keytabFile = keytab;

    String principal = mapContext.getString(
            SecurityConstants.AUTHENTICATION_KERBEROS_PRINCIPAL).trim();
    if (principal.length() == 0) {
      throw new SqoopException(SecurityError.AUTH_0002,
              SecurityConstants.AUTHENTICATION_KERBEROS_PRINCIPAL);
    }
    keytabPrincipal = principal;

    Configuration conf = new Configuration();
    conf.set(get_hadoop_security_authentication(),
            SecurityConstants.TYPE.KERBEROS.name());
    UserGroupInformation.setConfiguration(conf);
    try {
      String hostPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
      UserGroupInformation.loginUserFromKeytab(hostPrincipal, keytab);
    } catch (IOException ex) {
      throw new SqoopException(SecurityError.AUTH_0003, ex);
    }
    LOG.info("Using Kerberos authentication, principal ["
            + principal + "] keytab [" + keytab + "]");
  }
}
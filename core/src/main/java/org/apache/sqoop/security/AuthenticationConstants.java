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
package org.apache.sqoop.security;

import org.apache.sqoop.core.ConfigurationConstants;

/**
 * Constants that are used in authentication module.
 */
public final class AuthenticationConstants {

  /**
   * All authentication related configuration is prefixed with this:
   * <tt>org.apache.sqoop.authentication.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_CONFIG =
    ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "authentication.";

  /**
   * The config specifies the sqoop authentication type (SIMPLE, KERBEROS).
   * The default type is SIMPLE
   * <tt>org.apache.sqoop.authentication.type</tt>.
   */
  public static final String AUTHENTICATION_TYPE =
    PREFIX_AUTHENTICATION_CONFIG + "type";

  /**
   * The config specifies the sqoop authentication handler class.
   * The default type is org.apache.sqoop.security.SimpleAuthenticationHandler
   * <tt>org.apache.sqoop.authentication.handler</tt>.
   */
  public static final String AUTHENTICATION_HANDLER =
          PREFIX_AUTHENTICATION_CONFIG + "handler";

  /**
   * The config enables or disables anonymous authentication.
   * <tt>org.apache.sqoop.authentication.anonymous</tt>.
   */
  public static final String AUTHENTICATION_ANONYMOUS =
      PREFIX_AUTHENTICATION_CONFIG + "anonymous";

  /**
   * All kerberos authentication related configuration is prefixed with this:
   * <tt>org.apache.sqoop.authentication.kerberos.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_KERBEROS_CONFIG =
          PREFIX_AUTHENTICATION_CONFIG + "kerberos.";

  /**
   * The config specifies the kerberos principal.
   * <tt>org.apache.sqoop.authentication.kerberos.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_PRINCIPAL =
    PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "principal";

  /**
   * The config specifies the kerberos keytab.
   * <tt>org.apache.sqoop.authentication.kerberos.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_KEYTAB =
    PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "keytab";

  /**
   * All kerberos authentication for http related configuration is prefixed with this:
   * <tt>org.apache.sqoop.authentication.kerberos.http.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG =
          PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "http.";

  /**
   * The config specifies the kerberos principal for http.
   * <tt>org.apache.sqoop.authentication.kerberos.http.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_HTTP_PRINCIPAL =
          PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG + "principal";

  /**
   * The config specifies the kerberos keytab for http.
   * <tt>org.apache.sqoop.authentication.kerberos.http.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_HTTP_KEYTAB =
          PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG + "keytab";

  /**
   * The config specifies the token kind in delegation token.
   */
  public static final String TOKEN_KIND = "sqoop_token_kind";

  public static enum TYPE {SIMPLE, KERBEROS}

  private AuthenticationConstants() {
    // Instantiation of this class is prohibited
  }
}

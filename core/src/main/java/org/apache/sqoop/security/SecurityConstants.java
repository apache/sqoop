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
public final class SecurityConstants {
  /**
   * All security related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.</tt>
   */
  public static final String PREFIX_SECURITY_CONFIG =
          ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "security.";
  /**
   * All authentication related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.authentication.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_CONFIG =
    PREFIX_SECURITY_CONFIG + "authentication.";

  /**
   * All tls related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.tls.</tt>
   */
  public static final String PREFIX_TLS_CONFIG =
    PREFIX_SECURITY_CONFIG + "tls.";

  /**
   * All repository encryption related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.repo_encryption.</tt>
   */
  public static final String PREFIX_REPO_ENCRYPTION_CONFIG =
    PREFIX_SECURITY_CONFIG + "repo_encryption.";

  /**
   * The config specifies the sqoop authentication type (SIMPLE, KERBEROS).
   * The default type is SIMPLE
   * <tt>org.apache.sqoop.security.authentication.type</tt>.
   */
  public static final String AUTHENTICATION_TYPE =
    PREFIX_AUTHENTICATION_CONFIG + "type";

  /**
   * The config specifies the sqoop authentication handler class.
   * The default type is org.apache.sqoop.security.authentication.SimpleAuthenticationHandler
   * <tt>org.apache.sqoop.security.authentication.handler</tt>.
   */
  public static final String AUTHENTICATION_HANDLER =
          PREFIX_AUTHENTICATION_CONFIG + "handler";

  /**
   * The config enables or disables anonymous authentication.
   * <tt>org.apache.sqoop.security.authentication.anonymous</tt>.
   */
  public static final String AUTHENTICATION_ANONYMOUS =
      PREFIX_AUTHENTICATION_CONFIG + "anonymous";

  /**
   * All kerberos authentication related configuration is prefixed with this:
   * <tt>org.apache.security.sqoop.authentication.kerberos.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_KERBEROS_CONFIG =
          PREFIX_AUTHENTICATION_CONFIG + "kerberos.";

  /**
   * The config specifies the default user.
   */
  public static final String AUTHENTICATION_DEFAULT_USER =
      PREFIX_AUTHENTICATION_CONFIG + "default.user";

  public static final String AUTHENTICATION_DEFAULT_USER_DEFAULT =
      "sqoop.anonymous.user";

  /**
   * The config specifies the kerberos principal.
   * <tt>org.apache.sqoop.security.authentication.kerberos.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_PRINCIPAL =
    PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "principal";

  /**
   * The config specifies the kerberos keytab.
   * <tt>org.apache.sqoop.security.authentication.kerberos.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_KEYTAB =
    PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "keytab";

  /**
   * All kerberos authentication for http related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.authentication.kerberos.http.</tt>
   */
  public static final String PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG =
          PREFIX_AUTHENTICATION_KERBEROS_CONFIG + "http.";

  /**
   * The config specifies the kerberos principal for http.
   * <tt>org.apache.sqoop.security.authentication.kerberos.http.principal</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_HTTP_PRINCIPAL =
          PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG + "principal";

  /**
   * The config specifies the kerberos keytab for http.
   * <tt>org.apache.sqoop.security.authentication.kerberos.http.keytab</tt>.
   */
  public static final String AUTHENTICATION_KERBEROS_HTTP_KEYTAB =
          PREFIX_AUTHENTICATION_KERBEROS_HTTP_CONFIG + "keytab";

  /**
   * All authorization related configuration is prefixed with this:
   * <tt>org.apache.sqoop.security.authorization.</tt>
   */
  public static final String PREFIX_AUTHORIZATION_CONFIG =
          PREFIX_SECURITY_CONFIG + "authorization.";

  /**
   * The config specifies the sqoop authorization handler class.
   * The default type is org.apache.sqoop.security.authorization.DefaultAuthorizationHandler
   * <tt>org.apache.sqoop.security.authorization.handler</tt>.
   */
  public static final String AUTHORIZATION_HANDLER =
          PREFIX_AUTHORIZATION_CONFIG + "handler";

  /**
   * The config specifies the sqoop authorization access controller class.
   * The default type is org.apache.sqoop.security.authorization.DefaultAuthorizationAccessController
   * <tt>org.apache.sqoop.security.authorization.access_controller</tt>.
   */
  public static final String AUTHORIZATION_ACCESS_CONTROLLER =
          PREFIX_AUTHORIZATION_CONFIG + "access_controller";

  /**
   * The config specifies the sqoop authorization validator class.
   * The default type is org.apache.sqoop.security.authorization.DefaultAuthorizationValidator
   * <tt>org.apache.sqoop.security.authorization.validator</tt>.
   */
  public static final String AUTHORIZATION_VALIDATOR =
          PREFIX_AUTHORIZATION_CONFIG + "validator";

  /**
   * The config specifies the sqoop authentication provider class.
   * The default type is org.apache.sqoop.security.authorization.DefaultAuthenticationProvider
   * <tt>org.apache.sqoop.security.authorization.authentication_provider</tt>.
   */
  public static final String AUTHENTICATION_PROVIDER =
          PREFIX_AUTHORIZATION_CONFIG + "authentication_provider";

  /**
   * The config specifies the sqoop server name for authorization.
   * The default server name is "SqoopServer1"
   * <tt>org.apache.sqoop.security.authorization.server_name</tt>.
   */
  public static final String SERVER_NAME =
          PREFIX_AUTHORIZATION_CONFIG + "server_name";

  /**
   * The config specifies the whether the http server should use TLS.
   * <tt>org.apache.sqoop.security.tls.enabled</tt>.
   */
  public static final String TLS_ENABLED =
          PREFIX_TLS_CONFIG + "enabled";

  /**
   * The config specifies the tls protocol version
   * <tt>org.apache.sqoop.security.tls.protocol</tt>.
   */
  public static final String TLS_PROTOCOL =
    PREFIX_TLS_CONFIG + "protocol";

  /**
   * The config specifies the location of the JKS formatted keystore
   * <tt>org.apache.sqoop.security.tls.keystore</tt>.
   */
  public static final String KEYSTORE_LOCATION =
          PREFIX_TLS_CONFIG + "keystore";

  /**
   * The config specifies the password of the JKS formatted keystore
   * <tt>org.apache.sqoop.security.tls.keystore_password</tt>.
   */
  public static final String KEYSTORE_PASSWORD =
          PREFIX_TLS_CONFIG + "keystore_password";

  /**
   * The config specifies a script that outputs the password of the JKS formatted keystore
   * to standard out
   * <tt>org.apache.sqoop.security.tls.keystore_password_generator</tt>.
   */
  public static final String KEYSTORE_PASSWORD_GENERATOR =
    PREFIX_TLS_CONFIG + "keystore_password_generator";

  /**
   * The config specifies the password for the private key in the JKS formatted
   * keystore
   * <tt>org.apache.sqoop.security.tls.keymanagerpassword</tt>.
   */
  public static final String KEYMANAGER_PASSWORD =
    PREFIX_TLS_CONFIG + "keymanager_password";

  /**
   * The config specifies a script that outputs the password for the
   * private key in the JKS formatted keystore to standard out
   * <tt>org.apache.sqoop.security.tls.keymanager_password_generator</tt>.
   */
  public static final String KEYMANAGER_PASSWORD_GENERATOR =
    PREFIX_TLS_CONFIG + "keymanager_password_generator";

  /**
   * The config specifies if repository encryption is enabled
   * <tt>org.apache.sqoop.security.repo_encryption.enabled</tt>.
   */
  public static final String REPO_ENCRYPTION_ENABLED =
    PREFIX_REPO_ENCRYPTION_CONFIG + "enabled";

  /**
   * The config specifies the password used to encrypt the repository
   * <tt>org.apache.sqoop.security.repo_encryption.password</tt>.
   */
  public static final String REPO_ENCRYPTION_PASSWORD =
    PREFIX_REPO_ENCRYPTION_CONFIG + "password";

  /**
   * The config specifies a command that prints the password used to encrypt
   * the repository to standard out
   * <tt>org.apache.sqoop.security.repo_encryption.password_generator</tt>.
   */
  public static final String REPO_ENCRYPTION_PASSWORD_GENERATOR=
    PREFIX_REPO_ENCRYPTION_CONFIG + "password_generator";

  /**
   * The config specifies the algorithm to be used for hmac generation
   * <tt>org.apache.sqoop.security.repo_encryption.hmac_algorithm</tt>.
   */
  public static final String REPO_ENCRYPTION_HMAC_ALGORITHM=
    PREFIX_REPO_ENCRYPTION_CONFIG + "hmac_algorithm";

  /**
   * The config specifies the algorithm to be used for repository encryption
   * <tt>org.apache.sqoop.security.repo_encryption.cipher_algorithm</tt>.
   */
  public static final String REPO_ENCRYPTION_CIPHER_ALGORITHM=
    PREFIX_REPO_ENCRYPTION_CONFIG + "cipher_algorithm";

  /**
   * The config specifies the spec to be used for repository encryption
   * <tt>org.apache.sqoop.security.repo_encryption.cipher_spec</tt>.
   */
  public static final String REPO_ENCRYPTION_CIPHER_SPEC=
    PREFIX_REPO_ENCRYPTION_CONFIG + "cipher_spec";

  /**
   * The config specifies the size of the key used for repository encryption
   * <tt>org.apache.sqoop.security.repo_encryption.cipher_key_size</tt>.
   */
  public static final String REPO_ENCRYPTION_CIPHER_KEY_SIZE=
    PREFIX_REPO_ENCRYPTION_CONFIG + "cipher_key_size";

  /**
   * The config specifies the size of the initialization vector used for repository encryption
   * <tt>org.apache.sqoop.security.repo_encryption.initialization_vector_size</tt>.
   */
  public static final String REPO_ENCRYPTION_INITIALIZATION_VECTOR_SIZE=
    PREFIX_REPO_ENCRYPTION_CONFIG + "initialization_vector_size";

  /**
   * The config specifies the pbkdf2 algorithm to be used for master key generation
   * <tt>org.apache.sqoop.security.repo_encryption.pbkdf2_algorithm</tt>.
   */
  public static final String REPO_ENCRYPTION_PBKDF2_ALGORITHM=
    PREFIX_REPO_ENCRYPTION_CONFIG + "pbkdf2_algorithm";

  /**
   * The config specifies the number of rounds of the pbkdf2 algorithm
   * to be used for master key generation
   * <tt>org.apache.sqoop.security.repo_encryption.pbkdf2_algorithm</tt>.
   */
  public static final String REPO_ENCRYPTION_PBKDF2_ROUNDS=
    PREFIX_REPO_ENCRYPTION_CONFIG + "pbkdf2_rounds";

  /**
   * The config specifies the token kind in delegation token.
   */
  public static final String TOKEN_KIND = "sqoop_token_kind";

  public static enum TYPE {SIMPLE, KERBEROS}

  private SecurityConstants() {
    // Instantiation of this class is prohibited
  }
}

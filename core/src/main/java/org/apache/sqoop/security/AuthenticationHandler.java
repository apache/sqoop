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

import org.apache.log4j.Logger;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/***
 * AuthenticationHandler is responsible for secure checking.
 * KerberosAuthenticationHandler and SimpleAuthenticationHandler have
 * implemented this abstract class.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AuthenticationHandler {

  private static final Logger LOG = Logger.getLogger(AuthenticationHandler.class);

  /**
   * Security enabled option value
   */
  protected boolean securityEnabled = false;

  /**
   * AuthenticationProvider is an authentication to get userNames and groupNames.
   */
  protected AuthenticationProvider authenticationProvider;

  public boolean isSecurityEnabled() {
    return securityEnabled;
  }

  public AuthenticationProvider getAuthenticationProvider() {
    return authenticationProvider;
  }

  public abstract void doInitialize();

  public abstract void secureLogin();

  public String get_hadoop_security_authentication() {
    return "hadoop.security.authentication";
  }
}
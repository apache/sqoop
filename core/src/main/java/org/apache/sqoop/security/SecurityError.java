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

import org.apache.sqoop.common.ErrorCode;

public enum SecurityError implements ErrorCode {

  /** An unknown error has occurred. */
  AUTH_0000("An unknown error has occurred"),

  /** The system was not able to find Kerberos keytab in sqoop configuration. */
  AUTH_0001("Unable to find Kerberos keytab"),

  /** The system was not able to find Kerberos principal in sqoop configuration. */
  AUTH_0002("Unable to find Kerberos principal"),

  /** The system was not able to login using Kerberos keytab and principal in sqoop configuration. */
  AUTH_0003("Unable to login using Kerberos keytab and principal"),

  /** Invalid authentication type {simple, Kerberos}. */
  AUTH_0004("Invalid authentication type"),

  /** The system was not able to find Kerberos keytab for http in sqoop configuration. */
  AUTH_0005("Unable to find Kerberos keytab for http"),

  /** The system was not able to find Kerberos principal for http in sqoop configuration. */
  AUTH_0006("Unable to find Kerberos principal for http"),

  /** The system was not able to find authorization handler. */
  AUTH_0007("Unable to find authorization handler"),

  /** The system was not able to find authorization access controller. */
  AUTH_0008("Unable to find authorization access controller"),

  /** The system was not able to find authorization validator. */
  AUTH_0009("Unable to find authorization validator"),

  /** The system was not able to find authentication provider. */
  AUTH_0010("Unable to find authentication provider"),

  /** The system was not able to get authentication from http request. */
  AUTH_0011("Unable to get remote authentication from http request"),

  /** The system was not able to get role name from http request. */
  AUTH_0012("Unable to get role name from http request"),

  /** The system was not able to get principal from http request. */
  AUTH_0013("Unable to get principal from http request"),

  /** Authorization Exception, used by authorization implementation, etc. Sentry. */
  AUTH_0014("Authorization exception");

  private final String message;

  private SecurityError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}

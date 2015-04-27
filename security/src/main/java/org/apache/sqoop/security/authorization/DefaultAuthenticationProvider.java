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

package org.apache.sqoop.security.authorization;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.security.AuthenticationProvider;
import org.apache.sqoop.security.SecurityError;

public class DefaultAuthenticationProvider extends AuthenticationProvider {

  @Override
  public String[] getGroupNames() {
    return getRemoteUGI().getGroupNames();
  }

  @Override
  public String getUserName() {
    return getRemoteUGI().getShortUserName();
  }

  private UserGroupInformation getRemoteUGI() {
    UserGroupInformation ugi = null;
    try {
      ugi = HttpUserGroupInformation.get();
    } catch (Exception e) {
      throw new SqoopException(SecurityError.AUTH_0011,
              "Unable to get remote authentication from http request", e);
    }

    if (ugi == null) {
      throw new SqoopException(SecurityError.AUTH_0011,
              "Unable to get remote authentication from http request");
    }
    return ugi;
  }
}
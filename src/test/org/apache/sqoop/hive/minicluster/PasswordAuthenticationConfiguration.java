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

package org.apache.sqoop.hive.minicluster;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.sqoop.db.JdbcConnectionFactory;

import javax.security.sasl.AuthenticationException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS;

public class PasswordAuthenticationConfiguration implements AuthenticationConfiguration {

  private static String TEST_USERNAME;

  private static String TEST_PASSWORD;

  private static final class TestPasswordAuthenticationProvider implements PasswdAuthenticationProvider {

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
      if (!(TEST_USERNAME.equals(user) && TEST_PASSWORD.equals(password))) {
        throw new AuthenticationException("Authentication failed!");
      }
    }
  }

  public PasswordAuthenticationConfiguration(String testUsername, String testPassword) {
    TEST_USERNAME = testUsername;
    TEST_PASSWORD = testPassword;
  }

  @Override
  public Map<String, String> getAuthenticationConfig() {
    Map<String, String> result = new HashMap<>();
    result.put(HIVE_SERVER2_AUTHENTICATION.varname, HiveAuthFactory.AuthTypes.CUSTOM.getAuthName());
    result.put(HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname, TestPasswordAuthenticationProvider.class.getName());

    return result;
  }

  @Override
  public String getUrlParams() {
    return StringUtils.EMPTY;
  }

  @Override
  public <T> T doAsAuthenticated(PrivilegedAction<T> action) {
    return action.run();
  }

  @Override
  public void init() {
    //do nothing
  }

  @Override
  public JdbcConnectionFactory decorateConnectionFactory(JdbcConnectionFactory connectionFactory) {
    return connectionFactory;
  }
}

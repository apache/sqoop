/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive.minicluster;

import org.apache.sqoop.db.JdbcConnectionFactory;

import java.security.PrivilegedAction;
import java.util.Map;

public interface AuthenticationConfiguration {

  Map<String, String> getAuthenticationConfig();

  String getUrlParams();

  <T> T doAsAuthenticated(PrivilegedAction<T> action);

  void init();

  JdbcConnectionFactory decorateConnectionFactory(JdbcConnectionFactory connectionFactory);

}

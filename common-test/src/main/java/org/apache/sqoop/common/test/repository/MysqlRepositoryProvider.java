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

package org.apache.sqoop.common.test.repository;

import java.util.HashMap;
import java.util.Map;

public class MysqlRepositoryProvider extends RepositoryProviderBase {

  private static final String DRIVER = "com.mysql.jdbc.Driver";

  private static final String CONNECTION = System.getProperties().getProperty(
      "sqoop.repository.mysql.jdbc.url",
      "jdbc:mysql://localhost/test"
  );

  private static final String USERNAME = System.getProperties().getProperty(
      "sqoop.repository.mysql.username",
      "sqoop"
  );

  private static final String PASSWORD = System.getProperties().getProperty(
      "sqoop.repository.mysql.password",
      "sqoop"
  );

  @Override
  public Map<String, String> getPropertiesMap() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.repository.provider", "org.apache.sqoop.repository.JdbcRepositoryProvider");
    properties.put("org.apache.sqoop.repository.schema.immutable", "false");
    properties.put("org.apache.sqoop.repository.jdbc.handler", "org.apache.sqoop.repository.mysql.MySqlRepositoryHandler");
    properties.put("org.apache.sqoop.repository.jdbc.transaction.isolation", "READ_COMMITTED");
    properties.put("org.apache.sqoop.repository.jdbc.maximum.connections", "10");
    properties.put("org.apache.sqoop.repository.jdbc.url", CONNECTION);
    properties.put("org.apache.sqoop.repository.jdbc.driver", DRIVER);
    properties.put("org.apache.sqoop.repository.jdbc.user", USERNAME);
    properties.put("org.apache.sqoop.repository.jdbc.password", PASSWORD);

    return properties;
  }

}

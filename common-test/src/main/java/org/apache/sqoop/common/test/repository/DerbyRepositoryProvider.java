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

/**
This class encapsulates the logic around generating properties for using
derby as repository provider in Integration Tests
 */
public class DerbyRepositoryProvider extends RepositoryProviderBase {
  @Override
  public Map<String,String> getPropertiesMap() {
    Map<String, String> properties = new HashMap<String, String>();

    properties.put("org.apache.sqoop.repository.provider", "org.apache.sqoop.repository.JdbcRepositoryProvider");
    properties.put("org.apache.sqoop.repository.schema.immutable", "false");
    properties.put("org.apache.sqoop.repository.jdbc.handler", "org.apache.sqoop.repository.derby.DerbyRepositoryHandler");
    properties.put("org.apache.sqoop.repository.jdbc.transaction.isolation", "READ_COMMITTED");
    properties.put("org.apache.sqoop.repository.jdbc.maximum.connections", "10");
    properties.put("org.apache.sqoop.repository.jdbc.url=jdbc:derby:memory:myDB;create", "true");
    properties.put("org.apache.sqoop.repository.jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
    properties.put("org.apache.sqoop.repository.jdbc.user", "sa");
    properties.put("org.apache.sqoop.repository.jdbc.password", "");

    return properties;
  }
}

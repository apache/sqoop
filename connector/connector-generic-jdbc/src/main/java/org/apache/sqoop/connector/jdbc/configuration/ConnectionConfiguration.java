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
package org.apache.sqoop.connector.jdbc.configuration;

import org.apache.sqoop.model.Configuration;
import org.apache.sqoop.model.Input;

import java.util.Map;

import static org.apache.sqoop.connector.jdbc.GenericJdbcConnectorConstants.*;

/**
 *
 */
@Configuration
public class ConnectionConfiguration {
  @Input(form = FORM_CONNECTION, size = 128) public String jdbcDriver;
  @Input(form = FORM_CONNECTION, size = 128) public String connectionString;
  @Input(form = FORM_CONNECTION, size = 40)  public String username;
  @Input(form = FORM_CONNECTION, size = 40, sensitive = true) public String password;

  @Input(form = FORM_CONNECTION) public Map<String, String> jdbcProperties;

  //TODO(jarcec): Those parameters should be moved to job configuration!
  @Input(form = FORM_TABLE, size = 50) public String tableName;
  @Input(form = FORM_TABLE, size = 50) public String sql;
  @Input(form = FORM_TABLE, size = 50) public String columns;
  @Input(form = FORM_TABLE, size = 50) public String warehouse;
  @Input(form = FORM_TABLE, size = 50) public String dataDirectory;
  @Input(form = FORM_TABLE, size = 50) public String partitionColumn;
  @Input(form = FORM_TABLE, size = 50) public String boundaryQuery;
}

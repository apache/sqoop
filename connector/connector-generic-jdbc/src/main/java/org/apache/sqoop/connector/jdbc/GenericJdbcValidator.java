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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;

/**
 *
 */
public class GenericJdbcValidator extends Validator {

  @Override
  public Validation validateConnection(Object configuration) {
    Validation validation = new Validation(ConnectionConfiguration.class);
    ConnectionConfiguration config = (ConnectionConfiguration)configuration;

    if(config.connection.connectionString == null
      || !config.connection.connectionString.startsWith("jdbc:")) {
      validation.addMessage(Status.UNACCEPTABLE,
        "connection", "connectionString",
        "This do not seem as a valid JDBC URL");
    }

    return validation;
  }
}

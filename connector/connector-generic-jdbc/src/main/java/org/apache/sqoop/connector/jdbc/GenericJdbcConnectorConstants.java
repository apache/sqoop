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

/**
 *
 */
public final class GenericJdbcConnectorConstants {

  // Resource bundle name
  public static final String RESOURCE_BUNDLE_NAME =
      "generic-jdbc-connector-resources";

  // Metadata constants

  // Connection form
  public static final String FORM_CONNECTION = "form-connection";

  // Connection form inputs
  public static final String INPUT_CONN_JDBCDRIVER = "inp-conn-jdbcdriver";
  public static final String INPUT_CONN_CONNECTSTRING =
      "inp-conn-connectstring";
  public static final String INPUT_CONN_USERNAME = "inp-conn-username";
  public static final String INPUT_CONN_PASSWORD = "inp-conn-password";
  public static final String INPUT_CONN_JDBCPROPS = "inp-conn-jdbc-properties";

  // Table form
  public static final String FORM_TABLE = "form-table";

  // Table form inputs
  public static final String INPUT_TBL_TABLE = "inp-tbl-table";

  private GenericJdbcConnectorConstants() {
    // Disable explicit object creation
  }
}

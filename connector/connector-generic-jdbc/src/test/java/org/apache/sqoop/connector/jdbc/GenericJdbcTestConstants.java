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

import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;

public class GenericJdbcTestConstants {

  /**
   * Testing Driver
   */
  public static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

  /**
   * Testing database (in memory derby)
   */
  public static final String URL = "jdbc:derby:memory:TESTDB;create=true";

  /**
   * URL to drop the in-memory database
   */
  public static final String URL_DROP = "jdbc:derby:memory:TESTDB;drop=true";

  /**
   * Test link configuration
   */
  public static final LinkConfiguration LINK_CONFIGURATION = new LinkConfiguration();
  static {
    LINK_CONFIGURATION.linkConfig.jdbcDriver = DRIVER;
    LINK_CONFIGURATION.linkConfig.connectionString = URL;
    LINK_CONFIGURATION.linkConfig.fetchSize = 25;
  }
}

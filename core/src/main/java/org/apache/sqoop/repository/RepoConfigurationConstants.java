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
package org.apache.sqoop.repository;

import org.apache.sqoop.core.ConfigurationConstants;

public final class RepoConfigurationConstants {

  /**
   * All repository related configuration is prefixed with this:
   * <tt>org.apache.sqoop.repository.</tt>
   */
  public static final String PREFIX_REPO_CONFIG =
      ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "repository.";

  /**
   * System properties set up by the Repository Manager before initializing
   * the provider.
   */
  public static final String SYSCFG_REPO_SYSPROP_PREFIX = PREFIX_REPO_CONFIG
      + "sysprop.";

  /**
   * Class name of the repository implementation specified by:
   * <tt>org.apache.sqoop.repository.provider</tt>
   */
  public static final String SYSCFG_REPO_PROVIDER = PREFIX_REPO_CONFIG
      + "provider";

  /**
   * Boolean property defining whether repository is allowed to make changes
   * on disk structures (schema in databases, changing file format, ...).
   */
  public static final String SYSCFG_REPO_SCHEMA_IMMUTABLE=
    PREFIX_REPO_CONFIG + "schema.immutable";

  /**
   * Class name for the JDBC repository handler specified by:
   * <tt>org.apache.sqoop.repository.jdbc.handler</tt>.
   */
  public static final String SYSCFG_REPO_JDBC_HANDLER = PREFIX_REPO_CONFIG
      + "jdbc.handler";

  /**
   * JDBC connection URL specified by:
   * <tt>org.apache.sqoop.repository.jdbc.url</tt>
   */
  public static final String SYSCFG_REPO_JDBC_URL = PREFIX_REPO_CONFIG
      + "jdbc.url";

  /**
   * JDBC driver to be used, specified by:
   * <tt>org.apache.sqoop.repository.jdbc.driver</tt>
   */
  public static final String SYSCFG_REPO_JDBC_DRIVER = PREFIX_REPO_CONFIG
      + "jdbc.driver";

  /**
   * JDBC connection user name, specified by:
   * <tt>org.apache.sqoop.repository.jdbc.user</tt>
   */
  public static final String SYSCFG_REPO_JDBC_USER = PREFIX_REPO_CONFIG
      + "jdbc.user";

  /**
   * JDBC connection password, specified by:
   * <tt>org.apache.sqoop.repository.jdbc.password</tt>
   */
  public static final String SYSCFG_REPO_JDBC_PASSWORD = PREFIX_REPO_CONFIG
      + "jdbc.password";

  /**
   * JDBC Transaction Isolation, specified by:
   * <tt>org.apache.sqoop.repository.jdbc.transaction.isolation</tt>. The valid
   * values include: <tt>READ_UNCOMMITTED</tt>, <tt>READ_COMMITTED</tt>,
   * <tt>REPEATABLE_READ</tt> and <tt>SERIALIZABLE</tt>.
   */
  public static final String SYSCFG_REPO_JDBC_TX_ISOLATION = PREFIX_REPO_CONFIG
      + "jdbc.transaction.isolation";

  /**
   * JDBC connection pool maximum connections, specified by:
   * <tt>org.apache.sqoop.repository.jdbc.maximum.connections</tt>
   */
  public static final String SYSCFG_REPO_JDBC_MAX_CONN = PREFIX_REPO_CONFIG
      + "jdbc.maximum.connections";

  /**
   * Prefix that is used to provide any JDBC specific properties for the
   * system. Configuration keys which start with this prefix will be stripped
   * of the prefix and used as regular properties for JDBC connection
   * initialization. The prefix value is
   * <tt>org.apache.sqoop.repository.jdbc.properties.</tt> A property such as
   * <tt>foo</tt> with value <tt>bar</tt> will be set as:
   * <tt>org.apache.sqoop.repository.jdbc.properties.foo = bar </tt>
   */
  public static final String PREFIX_SYSCFG_REPO_JDBC_PROPERTIES =
      PREFIX_REPO_CONFIG + "jdbc.properties.";


  private RepoConfigurationConstants() {
    // Disable explicit object creation
  }
}

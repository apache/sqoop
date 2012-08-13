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
package org.apache.sqoop.core;

public final class ConfigurationConstants {

  /**
   * All configuration keys are prefixed with this:
   * <tt>org.apache.sqoop.</tt>
   */
  public static final String PREFIX_GLOBAL_CONFIG = "org.apache.sqoop.";

  /**
   * All logging related configuration is prefixed with this:
   * <tt>org.apache.sqoop.log4j.</tt>
   */
  public static final String PREFIX_LOG_CONFIG = PREFIX_GLOBAL_CONFIG
      + "log4j.";

  /**
   * Prefix for PropertiesConfigurationProvider implementation
   */
  public static final String PREFIX_PROPERTIES_PROVIDER_CONFIG =
      PREFIX_GLOBAL_CONFIG + "core.configuration.provider.properties.";

  /**
   * The system property that must be set for specifying the system
   * configuration directory: <tt>sqoop.config.dir</tt>.
   */
  public static final String SYSPROP_CONFIG_DIR = "sqoop.config.dir";

  /**
   * Bootstrap configuration property that specifies the system configuration
   * provider: <tt>sqoop.config.provider</tt>.
   */
  public static final String BOOTCFG_CONFIG_PROVIDER = "sqoop.config.provider";

  /**
   * Filename for the bootstrap configuration file:
   * <tt>sqoop_bootstrap.properties</tt>.
   */
  public static final String FILENAME_BOOTCFG_FILE =
      "sqoop_bootstrap.properties";


  public static final String FILENAME_CONNECTOR_PROPERTIES =
      "sqoopconnector.properties";

  public static final String CONPROP_PROVIDER_CLASS =
      "org.apache.sqoop.connector.class";

  public static final String CONNPROP_CONNECTOR_NAME =
      "org.apache.sqoop.connector.name";

  public static final String PROPERTIES_PROVIDER_SLEEP =
    PREFIX_PROPERTIES_PROVIDER_CONFIG + "sleep";


  private ConfigurationConstants() {
    // Disable explicit object creation
  }
}

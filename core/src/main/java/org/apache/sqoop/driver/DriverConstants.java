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
package org.apache.sqoop.driver;

import org.apache.sqoop.core.ConfigurationConstants;

/**
 * Constants that are used in driver module.
 */
public final class DriverConstants {

  // Sqoop driver constants

  public static final String PREFIX_SUBMISSION_CONFIG =
    ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "submission.";

  public static final String PREFIX_EXECUTION_CONFIG =
    ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "execution.";

  public static final String SYSCFG_SUBMISSION_ENGINE =
    PREFIX_SUBMISSION_CONFIG + "engine";

  public static final String PREFIX_SUBMISSION_ENGINE_CONFIG =
    SYSCFG_SUBMISSION_ENGINE + ".";

  public static final String PREFIX_SUBMISSION_PURGE_CONFIG =
    PREFIX_SUBMISSION_CONFIG + "purge.";

  public static final String SYSCFG_SUBMISSION_PURGE_THRESHOLD =
    PREFIX_SUBMISSION_PURGE_CONFIG + "threshold";

  public static final String SYSCFG_SUBMISSION_PURGE_SLEEP =
    PREFIX_SUBMISSION_PURGE_CONFIG + "sleep";

  public static final String PREFIX_SUBMISSION_UPDATE_CONFIG =
    PREFIX_SUBMISSION_CONFIG + "update.";

  public static final String SYSCFG_SUBMISSION_UPDATE_SLEEP =
    PREFIX_SUBMISSION_UPDATE_CONFIG + "sleep";

  public static final String SYSCFG_EXECUTION_ENGINE =
    PREFIX_EXECUTION_CONFIG + "engine";

  public static final String PREFIX_EXECUTION_ENGINE_CONFIG =
    SYSCFG_EXECUTION_ENGINE + ".";

  // Bundle name
  public static final String DRIVER_CONFIG_BUNDLE = "driver-config";

  private DriverConstants() {
    // Instantiation of this class is prohibited
  }
}

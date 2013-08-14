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
package org.apache.sqoop.audit;

import org.apache.sqoop.core.ConfigurationConstants;

public class AuditLoggerConstants {

  /**
   * All audit logger related configuration is prefixed with this:
   * <tt>org.apache.sqoop.auditlogger.</tt>
   */
  public static final String PREFIX_AUDITLOGGER_CONFIG =
      ConfigurationConstants.PREFIX_GLOBAL_CONFIG + "auditlogger.";

  /**
   * To specify an audit logger, the logger class must be given via
   * [LoggerName].provider.
   */
  public static final String SUFFIX_AUDITLOGGER_CLASS =
      ".class";
}

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

import java.util.Map;

import org.apache.sqoop.core.SqoopConfiguration;

/**
 * Interface to define an audit logger
 */
public abstract class AuditLogger {

  /**
   * The name of this logger
   */
  private String loggerName;

  /**
   * Initialize the logger
   */
  abstract void initialize();

  /**
   * Called to log an audit event
   *
   * @param username Name of the user executing the request.
   * @param ip Remote address of the request.
   * @param operation The type of the event.
   * @param objectType The type of the object to be operated upon.
   * @param objectId The id of the object to be operated upon.
   */
  abstract void logAuditEvent(String username, String ip,
      String operation, String objectType, String objectId);

  public String getLoggerName() {
    return loggerName;
  }

  public void setLoggerName(String loggerName) {
    this.loggerName = loggerName;
  }

  /**
   * Parse out all configurations for current logger
   * @return all configurations
   */
  protected Map<String, String> getLoggerConfig() {
    String prefix = AuditLoggerConstants.PREFIX_AUDITLOGGER_CONFIG + getLoggerName() + ".";
    return SqoopConfiguration.getInstance().getContext().getNestedProperties(prefix);
  }
}

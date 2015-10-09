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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.AuditLoggerError;

public class FileAuditLogger extends AuditLogger {

  private Logger LOG = Logger.getLogger(FileAuditLogger.class);

  /**
   * The option name for audit log file location
   */
  private static final String LOGGER = "logger";

  /**
   * The logger for output log lines
   */
  private Logger logger;

  public FileAuditLogger() {}

  public void initialize() {
    Map<String, String> config = getLoggerConfig();

    String loggerName = config.get(LOGGER);
    if (loggerName == null) {
      throw new SqoopException(AuditLoggerError.AUDIT_0002);
    }

    LOG.debug("Using audit logger name: " + loggerName);
    logger = Logger.getLogger(loggerName);
  }

  public void logAuditEvent(String username, String ip, String operation, String objectType, String objectId) {
    StringBuilder sentence = new StringBuilder();
    sentence.append("user=").append(username).append("\t");
    sentence.append("ip=").append(ip).append("\t");
    sentence.append("op=").append(operation).append("\t");
    sentence.append("obj=").append(objectType).append("\t");
    sentence.append("objId=").append(objectId);
    logger.info(sentence.toString());
  }
}

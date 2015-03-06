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
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.AuditLoggerError;

public class FileAuditLogger extends AuditLogger {

  private Logger LOG = Logger.getLogger(FileAuditLogger.class);

  private static final String APPENDER_SURFIX = "Appender";

  /**
   * The option name for audit log file location
   */
  private static final String FILE = "file";

  /**
   * The logger for output log lines
   */
  private Logger logger;

  /**
   * Configurations for this audit logger
   */
  private Map<String, String> config;

  /**
   * Properties to setup logger
   */
  private Properties props;

  public FileAuditLogger() {
    this.props = new Properties();
  }

  public void initialize() {
    config = getLoggerConfig();

    String outputFile = config.get(FILE);
    if (outputFile == null) {
      throw new SqoopException(AuditLoggerError.AUDIT_0002);
    }

    // setup logger
    String appender = "log4j.appender." + getLoggerName() + APPENDER_SURFIX;
    LOG.warn("appender: " + appender);
    props.put(appender, "org.apache.log4j.RollingFileAppender");
    props.put(appender + ".File", outputFile);
    props.put(appender + ".layout", "org.apache.log4j.PatternLayout");
    props.put(appender + ".layout.ConversionPattern", "%d %-5p %c: %m%n");
    props.put("log4j.logger." + getLoggerName(), "INFO," + getLoggerName() + APPENDER_SURFIX);
    PropertyConfigurator.configure(props);

    logger = Logger.getLogger(getLoggerName());
  }

  public void logAuditEvent(String username, String ip, String operation, String objectType,
          String objectId) {
    StringBuilder sentence = new StringBuilder();
    sentence.append("user=").append(username).append("\t");
    sentence.append("ip=").append(ip).append("\t");
    sentence.append("op=").append(operation).append("\t");
    sentence.append("obj=").append(objectType).append("\t");
    sentence.append("objId=").append(objectId);
    logger.info(sentence.toString());
  }
}

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

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.JobManager;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.security.AuthenticationManager;
import org.apache.sqoop.security.AuthorizationManager;

/**
 * Entry point for initializing and destroying Sqoop server
 */
public class SqoopServer {

  private static final Logger LOG = Logger.getLogger(SqoopServer.class);

  public static void destroy() {
    LOG.info("Shutting down Sqoop server");
    JobManager.getInstance().destroy();
    Driver.getInstance().destroy();
    ConnectorManager.getInstance().destroy();
    RepositoryManager.getInstance().destroy();
    AuditLoggerManager.getInstance().destroy();
    AuthorizationManager.getInstance().destroy();
    AuthenticationManager.getInstance().destroy();
    SqoopConfiguration.getInstance().destroy();
    LOG.info("Sqoop server has been correctly terminated");
  }

  public static void initialize() {
    try {
      LOG.info("Booting up Sqoop server");
      SqoopConfiguration.getInstance().initialize();
      AuthenticationManager.getInstance().initialize();
      AuthorizationManager.getInstance().initialize();
      AuditLoggerManager.getInstance().initialize();
      RepositoryManager.getInstance().initialize();
      ConnectorManager.getInstance().initialize();
      Driver.getInstance().initialize();
      JobManager.getInstance().initialize();
      LOG.info("Sqoop server has successfully boot up");
    } catch (Exception ex) {
      LOG.error("Server startup failure", ex);
      throw new RuntimeException("Failure in server initialization", ex);
    }
  }
}

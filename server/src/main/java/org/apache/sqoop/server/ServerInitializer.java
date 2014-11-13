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
package org.apache.sqoop.server;

import org.apache.log4j.Logger;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.SqoopServer;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Initializes the Sqoop server. This listener is also responsible for
 * cleaning up any resources occupied by the server during the system shutdown.
 */
public class ServerInitializer implements ServletContextListener {

  private static final Logger LOG = Logger.getLogger(ServerInitializer.class);

  public void contextDestroyed(ServletContextEvent arg0) {
    SqoopServer.destroy();
  }

  public void contextInitialized(ServletContextEvent arg0) {
    try {
      SqoopServer.initialize();
    } catch (Throwable ex) {
      // We are assuming that by default we are running as the only app inside
      // the tomcat and hence we want to try kill entire tomcat on our load failure.
      if("true".equals(System.getProperty(ConfigurationConstants.KILL_TOMCAT_ON_FAILURE, "true"))) {
        LOG.error("Sqoop server failed to start", ex);
        System.exit(1);
      }

      throw new RuntimeException("Sqoop server failed to start.", ex);
    }
  }
}

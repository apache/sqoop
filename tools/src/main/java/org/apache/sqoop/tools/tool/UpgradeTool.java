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
package org.apache.sqoop.tools.tool;

import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.tools.ConfiguredTool;
import org.apache.log4j.Logger;

/**
 * Upgrade all versionable components inside Sqoop2. This includes any
 * structural changes inside repository and the Connector and Driver entity data
 * This tool is idempotent.
 */
public class UpgradeTool extends ConfiguredTool {

  public static final Logger LOG = Logger.getLogger(UpgradeTool.class);

  @Override
  public boolean runToolWithConfiguration(String[] arguments) {
    try {
      LOG.info("Initializing the RepositoryManager with immutable option turned off.");
      RepositoryManager.getInstance().initialize(false);

      LOG.info("Initializing the Connection Manager with upgrade option turned on.");
      ConnectorManager.getInstance().initialize(true);

      LOG.info("Initializing the Driver with upgrade option turned on.");
      Driver.getInstance().initialize(true);

      LOG.info("Upgrade completed successfully.");

      LOG.info("Tearing all managers down.");
      ConnectorManager.getInstance().destroy();
      Driver.getInstance().destroy();
      RepositoryManager.getInstance().destroy();
      return true;
    } catch (Exception ex) {
      LOG.error("Can't finish upgrading RepositoryManager, Driver and ConnectionManager:", ex);
      System.out.println("Upgrade has failed, please check Server logs for further details.");
      return false;
    }
  }

}
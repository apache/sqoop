/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sqoop.db.DriverManagerJdbcConnectionFactory;

import java.io.IOException;
import java.sql.Connection;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class HiveServer2ConnectionFactory extends DriverManagerJdbcConnectionFactory {

  private static final Log LOG = LogFactory.getLog(HiveServer2ConnectionFactory.class.getName());

  private static final String HS2_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

  public HiveServer2ConnectionFactory(String connectionString, String username, String password) {
    super(HS2_DRIVER_CLASS, connectionString, username, password);
  }

  public HiveServer2ConnectionFactory(String connectionString, String username) {
    this(connectionString, username, null);
  }

  @Override
  public Connection createConnection() {
    LOG.info("Creating connection to HiveServer2 as: " + getCurrentUser());
    return super.createConnection();
  }

  private String getCurrentUser() {
    try {
      return UserGroupInformation.getCurrentUser().toString();
    } catch (IOException e) {
      LOG.error("Unable to determine current user.", e);
    }
    return EMPTY;
  }

}

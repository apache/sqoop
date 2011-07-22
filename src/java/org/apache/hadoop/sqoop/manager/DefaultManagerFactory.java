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

package org.apache.hadoop.sqoop.manager;

import org.apache.hadoop.sqoop.ImportOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains instantiation code for all ConnManager implementations
 * shipped and enabled by default in Sqoop.
 */
public final class DefaultManagerFactory implements ManagerFactory {

  public static final Log LOG = LogFactory.getLog(DefaultManagerFactory.class.getName());

  public ConnManager accept(ImportOptions options) {
    String manualDriver = options.getDriverClassName();
    if (manualDriver != null) {
      // User has manually specified JDBC implementation with --driver.
      // Just use GenericJdbcManager.
      return new GenericJdbcManager(manualDriver, options);
    }

    String connectStr = options.getConnectString();

    int schemeStopIdx = connectStr.indexOf("//");
    if (-1 == schemeStopIdx) {
      // no scheme component?
      LOG.warn("Could not parse connect string: [" + connectStr
          + "]; this may be malformed.");
      return null;
    }

    String scheme = connectStr.substring(0, schemeStopIdx);

    if (null == scheme) {
      // We don't know if this is a mysql://, hsql://, etc.
      // Can't do anything with this.
      LOG.warn("Null scheme associated with connect string.");
      return null;
    }

    if (scheme.equals("jdbc:mysql:")) {
      if (options.isDirect()) {
        return new LocalMySQLManager(options);
      } else {
        return new MySQLManager(options);
      }
    } else if (scheme.startsWith("jdbc:hsqldb:")) {
      return new HsqldbManager(options);
    } else if (scheme.startsWith("jdbc:oracle:")) {
      return new OracleManager(options);
    } else {
      return null;
    }
  }
}


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

package org.apache.sqoop.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.manager.ConnManager;

import static org.apache.sqoop.manager.SupportedManagers.CUBRID;
import static org.apache.sqoop.manager.SupportedManagers.DB2;
import static org.apache.sqoop.manager.SupportedManagers.HSQLDB;
import static org.apache.sqoop.manager.SupportedManagers.JTDS_SQLSERVER;
import static org.apache.sqoop.manager.SupportedManagers.MYSQL;
import static org.apache.sqoop.manager.SupportedManagers.NETEZZA;
import static org.apache.sqoop.manager.SupportedManagers.ORACLE;
import static org.apache.sqoop.manager.SupportedManagers.POSTGRES;
import static org.apache.sqoop.manager.SupportedManagers.SQLSERVER;


/**
 * Contains instantiation code for all ConnManager implementations
 * shipped and enabled by default in Sqoop.
 */
public class DefaultManagerFactory
    extends com.cloudera.sqoop.manager.ManagerFactory {

  public static final Log LOG = LogFactory.getLog(
      DefaultManagerFactory.class.getName());
  public static final String NET_SOURCEFORGE_JTDS_JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

  public ConnManager accept(JobData data) {
    SqoopOptions options = data.getSqoopOptions();

    String scheme = extractScheme(options);
    if (null == scheme) {
      // We don't know if this is a mysql://, hsql://, etc.
      // Can't do anything with this.
      LOG.warn("Null scheme associated with connect string.");
      return null;
    }

    LOG.debug("Trying with scheme: " + scheme);

    if (MYSQL.isTheManagerTypeOf(options)) {
      if (options.isDirect()) {
        return new DirectMySQLManager(options);
      } else {
        return new MySQLManager(options);
      }
    } else if (POSTGRES.isTheManagerTypeOf(options)) {
      if (options.isDirect()) {
        return new DirectPostgresqlManager(options);
      } else {
        return new PostgresqlManager(options);
      }
    } else if (HSQLDB.isTheManagerTypeOf(options)) {
      return new HsqldbManager(options);
    } else if (ORACLE.isTheManagerTypeOf(options)) {
      return new OracleManager(options);
    } else if (SQLSERVER.isTheManagerTypeOf(options)) {
      return new SQLServerManager(options);
    } else if (JTDS_SQLSERVER.isTheManagerTypeOf(options)) {
      return new SQLServerManager(NET_SOURCEFORGE_JTDS_JDBC_DRIVER, options);
    } else if (DB2.isTheManagerTypeOf(options)) {
      return new Db2Manager(options);
    } else if (NETEZZA.isTheManagerTypeOf(options)) {
      if (options.isDirect()) {
        return new DirectNetezzaManager(options);
      } else {
        return new NetezzaManager(options);
      }
    } else if (CUBRID.isTheManagerTypeOf(options)) {
      return new CubridManager(options);
    } else {
      return null;
    }
  }

  protected String extractScheme(SqoopOptions options) {
  return SupportedManagers.extractScheme(options);
  }
}

